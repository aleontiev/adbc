from asyncio import gather
from adbc.symbols import insert, delete
import json


class WithAlterSQL(object):
    def get_alter_constraint_query(
        self, table_name, name, deferred=None, deferrable=None, schema=None
    ):
        remainder = []
        if deferrable is not None:
            remainder.append("DEFERRABLE" if deferrable else "NOT DEFERRABLE")
        if deferred is not None:
            remainder.append(
                "INITIALLY DEFERRED" if deferred else "INITIALLY IMMEDIATE"
            )
        remainder = " ".join(remainder)
        if not remainder:
            return []

        table = self.F.table(table_name, schema=schema)
        constraint = self.F.constraint(name)
        return (f"ALTER TABLE {table}\nALTER CONSTRAINT {constraint} {remainder}",)

    def get_alter_column_query(
        self, table, column, null=None, type=None, schema=None, **kwargs
    ):
        has_default = "default" in kwargs
        default = kwargs.get("default") if has_default else None
        remainder = ""
        if type is not None:
            remainder = f"TYPE {type}"
        elif null is not None:
            remainder = f'{"DROP" if null else "SET"} NOT NULL'
        elif has_default:
            remainder = (
                f'{"SET" if default is not None else "DROP"} DEFAULT '
                f'{default if default is not None else ""}'
            )
        if not remainder:
            return []
        table = self.F.table(table, schema=schema)
        column = self.F.column(column)
        # preql: {
        #   "alter": {
        #       "column": {
        #           "name": "public.a.b",
        #           "default": default,
        #           "type": "type",
        #           ""
        #       }
        #  }
        return (f"ALTER TABLE {table}\nALTER COLUMN {column} {remainder}",)


class WithMerge(WithAlterSQL):
    async def alter_column(self, table, name, patch=None, schema=None):
        patch = patch or {}
        query = self.get_alter_column_query(
            table, name, schema=schema, **patch
        )
        await self.execute(*query)
        return True

    def _translate_schemas(self, d, scope, to):
        translate = self.get_scope_translation(
            scope=scope, to=to
        ) or {}
        if isinstance(d, dict):
            new = {}
            for schema_name, schema in d.items():
                new_schema_name = translate.get(schema_name, schema_name)
                child_scope = self.get_child_scope(new_schema_name, scope=scope)
                new[new_schema_name] = self._translate_tables(schema, child_scope, to)
            return new
        raise NotImplementedError()

    def _translate_tables(self, d, scope, to):
        translate = self.get_scope_translation(
            scope=scope, to=to, child_key='tables'
        ) or {}
        if isinstance(d, dict):
            new = {}
            for table_name, table in d.items():
                new_table_name = translate.get(table_name, table_name)
                child_scope = self.get_child_scope(
                    new_table_name,
                    scope=scope,
                    child_key='tables'
                )
                new[new_table_name] = table
                for child_key in ('indexes', 'constraints', 'columns'):
                    new[new_table_name][child_key] = self._translate(
                        child_key,
                        table.get(child_key, {}),
                        child_scope,
                        to
                    )
            return new
        raise NotImplementedError()

    def _translate(self, level, d, scope, to=None):
        """Copy helper: provides deep translation of named schematics"""
        if not to:
            to = self.tag

        if level == 'schemas':
            # translate schemas +... tables +... table items
            return self._translate_schemas(d, scope, to)
        elif level == 'tables':
            # translate tables +... table items
            return self._translate_tables(d, scope, to)
        elif level == 'indexes' or level == 'constraints' or level == 'columns':
            # translate table items: base case
            translate = self.get_scope_translation(
                scope=scope, to=to, child_key=level
            )
            if isinstance(d, dict):
                if not translate:
                    return d
                new = {}
                for k, v in d.items():
                    t = translate.get(k, k)
                    new[t] = v
                return new
            raise NotImplementedError()
        else:
            raise ValueError(f'translate: unexpected level "{level}"')


    async def merge(self, diff, level, parents=None, parallel=True, scope=None):
        if not diff:
            # both schemas are identical
            return {}

        if parents:
            self.log(f'merge: {".".join(parents)}.{level}s {diff}')
        else:
            self.log(f"merge: {level}s {diff}")

        plural = f"{level}s" if level[-1] != "x" else f"{level}es"
        create_all = getattr(self, f"create_{plural}")
        drop_all = getattr(self, f"drop_{plural}")
        merge = getattr(self, f"merge_{level}", None)

        if isinstance(diff, (list, tuple)):
            assert len(diff) == 2

            # source and target have no overlap
            # -> copy by dropping all in target not in source
            # and creating all in source not in target
            create, drop = diff

            create = self._translate(plural, create, scope)
            drop = self._translate(plural, drop, scope)

            # do these two actions in parallel
            inserted, deleted = await gather(
                create_all(create, parents=parents), drop_all(drop, parents=parents)
            )
            result = {}
            if inserted:
                result[insert] = inserted
            if deleted:
                result[delete] = deleted
            return result
        else:
            assert isinstance(diff, dict)

            diff = self._translate(plural, diff, scope)

            routines = []
            names = []
            results = []
            for name, changes in diff.items():
                action = None

                if name == delete:
                    action = create_all(changes, parents=parents)
                    names.append(insert)
                elif name == insert:
                    action = drop_all(changes, parents=parents)
                    names.append(delete)
                elif merge:
                    child_scope = self.get_child_scope(name, scope=scope, child_key=plural)
                    action = merge(name, changes, parents=parents, scope=child_scope)
                    names.append(name)

                if action:
                    if not parallel:
                        results.append(await action)
                    else:
                        routines.append(action)

            if routines:
                results = await gather(*routines)
            return {r[0]: r[1] for r in zip(names, results) if r[1] is not None}

    async def merge_constraint(self, name, diff, parents=None, scope=None):
        kwargs = {}
        kwargs = {}
        if "deferred" in diff:
            kwargs["deferred"] = diff["deferred"][0]
            diff['deferred'] = list(reversed(diff['deferred']))
        if "deferrable" in diff:
            kwargs["deferrable"] = diff["deferrable"][0]
            diff['deferrable'] = list(reversed(diff['deferrable']))

        if not kwargs:
            raise Exception(
                f"expecting constraint diff to have deferrable or deferred, is: {diff}"
            )

        if len(parents) == 1:
            schema_name = None
            table_name = parents[0]
        else:
            schema_name, table_name = parents

        query = self.get_alter_constraint_query(
            table_name, name, schema=schema_name, **kwargs
        )
        await self.execute(*query)
        return diff

    async def merge_index(self, column, diff, parents=None, scope=None):
        raise NotImplementedError()

    async def merge_column(self, column, diff, parents=None, scope=None):
        kwargs = {}
        if "null" in diff:
            kwargs["null"] = diff["null"][0]
            diff['null'] = list(reversed(diff['null']))
        if "type" in diff:
            kwargs["type"] = diff["type"][0]
            diff['type'] = list(reversed(diff['type']))
        if "default" in diff:
            kwargs["default"] = diff["default"][0]
            diff['default'] = list(reversed(diff['default']))

        if not kwargs:
            # can only change null, type, or default
            return None

        if len(parents) == 1:
            schema_name = None
            table_name = parents[0]
        else:
            schema_name, table_name = parents

        query = self.get_alter_column_query(
            table_name, column, schema=schema_name, **kwargs
        )
        await self.execute(*query)
        return diff

    async def merge_table(self, table_name, diff, parents=None, scope=None):
        parents = parents + [table_name]
        return {
            plural: await self.merge(diff[plural], child, parents, parallel=False, scope=scope)
            for child, plural in (
                ("column", "columns"),
                ("constraint", "constraints"),
                ("index", "indexes"),
            )
            if diff.get(plural)
        }

    async def merge_schema(self, schema_name, diff, parents=None, scope=None):
        # merge schemas in diff (have tables in common but not identical)
        return await self.merge(diff, "table", parents + [schema_name], scope=scope)
