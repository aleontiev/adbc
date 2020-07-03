from asyncio import gather
from jsondiff.symbols import insert, delete


class WithAlterSQL(object):
    def get_alter_constraint_query(
        self, table_name, name, deferred=None, deferrable=None, schema=None
    ):
        remainder = []
        if deferrable is not None:
            remainder.append("DEFERRABLE" if deferrable else "NOT DEFERRABLE")
        if deferred is not None:
            remainder.append(
                "INTIIALLY DEFERRED" if deferred else "INITIALLY IMMEDIATE"
            )
        remainder = " ".join(remainder)
        if not remainder:
            return []

        table = self.F.table(table_name, schema=schema)
        constraint = self.F.constraint(name)
        return (f"ALTER TABLE {table}\n" f"ALTER CONSTRAINT {constraint} {remainder}",)

    def get_alter_column_query(
        self, table, column, not_null=None, type=None, schema=None, **kwargs
    ):
        has_default = "default" in kwargs
        default = kwargs.get("default") if has_default else None
        remainder = ""
        if type is not None:
            remainder = f"TYPE {type}"
        elif not_null is not None:
            remainder = f'{"SET" if not_null else "DROP"} NOT NULL'
        elif has_default:
            remainder = (
                f'{"SET" if default is not None else "DROP"} DEFAULT '
                f'{default if default is not None else ""}'
            )
        if not remainder:
            return []
        table = self.F.table(table, schema=schema)
        column = self.F.column(column)
        return (f"ALTER TABLE {table} ALTER COLUMN {column} {remainder}",)


class WithMerge(WithAlterSQL):
    async def merge(self, diff, level, parents=None, parallel=True):
        if not diff:
            # both schemas are identical
            return {}

        if parents:
            self.log(f'merge: {".".join(parents)} {level}s')
        else:
            self.log(f"merge: all {level}s")

        plural = f"{level}s" if level[-1] != "x" else f"{level}es"
        create_all = getattr(self, f"create_{plural}")
        drop_all = getattr(self, f"drop_{plural}")
        merge = getattr(self, f"merge_{level}", None)

        if isinstance(diff, (list, tuple)):
            assert len(diff) == 2

            # source and target have no overlap
            # -> copy by dropping all in target not in source
            # and creating all in source not in target
            source, target = diff
            # do these two actions in parallel
            inserted, deleted = await gather(
                create_all(source, parents=parents), drop_all(source, parents=parents)
            )
            return {insert: inserted, delete: deleted}
        else:
            assert isinstance(diff, dict)

            routines = []
            names = []
            results = []
            for name, changes in diff.items():
                action = None
                if name == delete:
                    action = create_all(changes, parents=parents)
                elif name == insert:
                    action = drop_all(changes, parents=parents)
                elif merge:
                    action = merge(name, changes, parents=parents)

                if action:
                    if not parallel:
                        results.append(await action)
                    else:
                        routines.append(action)
                    names.append(name)

            if routines:
                results = await gather(*routines)
            return {r[0]: r[1] for r in zip(names, results)}

    async def merge_constraint(self, name, diff, parents=None):
        kwargs = {}
        schema_name, table_name = parents
        kwargs = {}
        if "deferred" in diff:
            kwargs["deferred"] = diff["deferred"][0]
        if "deferrable" in diff:
            kwargs["deferrable"] = diff["deferrable"][0]
        if not kwargs:
            raise Exception(
                f"expecting constraint diff to have deferrable or deferred, is: {diff}"
            )

        query = self.get_alter_constraint_query(
            table_name, name, schema=schema_name, **kwargs
        )
        await self.target.execute(*query)
        return diff

    async def merge_index(self, column, diff, parents=None):
        raise NotImplementedError()

    async def merge_column(self, column, diff, parents=None):
        kwargs = {}
        if "null" in diff:
            kwargs["not null"] = not diff["null"][0]
        if "type" in diff:
            kwargs["type"] = diff["type"][0]
        if "default" in diff:
            kwargs["default"] = diff["default"][0]
        if not kwargs:
            raise Exception("expecting column diff to have null, type, or default")
        schema_name, table_name = parents
        query = self.get_alter_column_query(
            table_name, column, schema=schema_name, **kwargs
        )
        await self.target.execute(*query)
        return diff

    async def merge_table(self, table_name, diff, parents=None):
        parents = parents + [table_name]
        diff = diff.get("schema", {})
        return [
            await self.merge(diff[plural], child, parents, parallel=False)
            for child, plural in (
                ("column", "columns"),
                ("constraint", "constraints"),
                ("index", "indexes"),
            )
            if diff.get(plural)
        ]

    async def merge_schema(self, schema_name, diff, parents=None):
        # merge schemas in diff (have tables in common but not identical)
        return await self.merge(diff, "table", parents + [schema_name])
