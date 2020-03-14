from adbc.utils import is_dsn
from adbc.database import Database
from asyncio import gather
from jsondiff.symbols import delete, insert
from adbc.store import Loggable


CONSTRAINT_TYPE_MAP = {
    "x": "EXCLUDE",
    "p": "PRIMARY KEY",
    "f": "FOREIGN KEY",
    "u": "UNIQUE",
    "c": "CHECK",
}


class Workflow(Loggable):
    def __init__(self, name, config, databases, verbose=False):
        self.name = name
        self.config = config
        self.databases = databases
        self.verbose = verbose
        steps = config.get("steps", [])
        if not steps:
            raise ValueError(f'workflow "{name}" has no steps')
        self.steps = [WorkflowStep(self, step) for step in steps]

    async def execute(self):
        results = []
        for step in self.steps:
            results.append(await step.execute())
        return results


class WorkflowStep(Loggable):
    def __new__(cls, workflow, config):
        if cls is WorkflowStep:
            command = config.get("command", "").lower()
            if not command:
                raise Exception(f'"command" is required but not provided')
            if command == "copy":
                return CopyStep(workflow, config)
            elif command == "diff":
                return DiffStep(workflow, config)
            elif command == "info":
                return InfoStep(workflow, config)
            else:
                raise Exception(f'the provided command "{command}" is not supported')
        else:
            return super(WorkflowStep, cls).__new__(cls)

    def __init__(self, workflow, config):
        self.workflow = workflow
        self.verbose = self.workflow.verbose
        self.config = config
        self.validate()

    def validate(self):
        raise NotImplementedError()

    async def execute(self):
        raise NotImplementedError()

    def validate_database_url(self, name):
        databases = self.workflow.databases
        if name not in databases:
            raise Exception(f'The provided name "{name}" is not defined in "databases"')

        url = databases[name].get("url")
        if not url:
            raise Exception(f'The database info for "{name}" does not include a URL')

        if not is_dsn(url):
            raise Exception(
                f'The value provided for database "{name}"'
                f' is not a valid URL: "{url}"'
            )

        return url

    def validate_database_config(self, name):
        databases = self.workflow.databases
        if name not in databases:
            raise Exception(f'The provided name "{name}" is not defined in "databases"')
        return databases[name]

    def _validate(self, name, read=False, write=False, alter=False):
        config = self.config
        datasource = config.get(name)
        if not datasource:
            raise Exception(f'"{name}" is required')

        url = self.validate_database_url(datasource)
        config = self.validate_database_config(datasource)
        database = self.validate_connection(
            name, url, config, read=read, write=write, alter=alter
        )
        setattr(self, name, database)

    def validate_connection(
        self, name, url, config, read=False, write=False, alter=False
    ):
        # TODO: validate read/write/alter permissions
        # for a faster / more proactive error message
        return Database(name=name, url=url, config=config, verbose=self.verbose)


class CopyStep(WorkflowStep):
    def validate(self):
        self._validate("source", read=True)
        self._validate("target", read=True, write=True, alter=True)

        self.translate = self.config.get("translate", None)
        drop_all = self.config.get("drop", False)
        self._drop_schemas = self.config.get("drop_schemas", False) or drop_all
        self._drop_tables = self.config.get("drop_tables", False) or drop_all
        self._drop_columns = self.config.get("drop_columns", True) or drop_all
        self._drop_constraints = self.config.get("drop_constraints", True) or drop_all
        self._drop_indexes = self.config.get("drop_indexes", True) or drop_all

    async def execute(self):
        translate = self.translate
        source = self.source
        target = self.target
        schema_diff = await source.diff(target, translate, only='schema')
        meta_changes = await self.copy_metadata(schema_diff)
        data_diff = await source.diff(target, translate, only='data')
        data_changes = await self.copy_data(data_diff)
        final_diff = await source.diff(target, translate)
        return {
            "schema_diff": schema_diff,
            "data_diff": data_diff,
            "meta_changes": meta_changes,
            "data_changes": data_changes,
            "final_diff": final_diff,
        }

    def get_create_schema_query(self, schema):
        return (f'CREATE SCHEMA "{schema}"',)

    def get_drop_schema_query(self, schema):
        return (f'DROP SCHEMA "{schema}" CASCADE',)

    def get_drop_table_query(self, schema, table):
        return (f'DROP TABLE "{schema}"."{table}" CASCADE',)

    def get_drop_column_query(self, schema, table, name):
        return (f'ALTER TABLE "{schema}"."{table}"\n' f"DROP COLUMN {name} CASCADE",)

    def get_drop_constraint_query(self, schema, table, name):
        return (
            f'ALTER TABLE "{schema}"."{table}"\n' f"DROP CONSTRAINT {name} CASCADE",
        )

    def get_drop_index_query(self, schema, name):
        return f'DROP INDEX "{schema}"."{name}" CASCADE'

    def get_create_index_query(self, schema, table, name, index):
        index = self.get_index
        unique = " UNIQUE" if index["unique"] else ""
        columns = ", ".join([f'"{c}"' for c in index["columns"]])
        type = index["type"]
        return (f"CREATE{unique} INDEX {name} ON {table} ({columns}) USING {type}",)

    def get_create_constraint_query(self, schema, table, name, constraint):
        constraint = self.get_constraint_sql(name, constraint)
        return f'ALTER TABLE "{schema}"."{table}"\n' f"ADD {constraint}"

    def get_create_column_query(self, schema, table, name, column):
        column = self.get_column_sql(name, column)
        return f'ALTER TABLE "{schema}"."{table}"\n' f"ADD COLUMN {column}"

    def get_create_table_columns_sql(self, table, spaces=2):
        table_schema = table.get("schema", {})
        columns = table_schema.get("columns", {})
        if not columns:
            return ""

        sqls = []
        for name, column in columns.items():
            sqls.append(self.get_column_sql(name, column))

        spaces = " " * spaces
        sqls = f",\n{spaces}".join(sqls)
        return f"{spaces}{sqls}"

    def get_create_table_constraints_sql(self, table, spaces=2):
        table_schema = table.get("schema", {})
        constraints = table_schema.get("constraints", {})

        if not constraints:
            return ""

        sqls = []
        for name, constraint in constraints.items():
            sqls.append(self.get_constraint_sql(name, constraint))

        spaces = " " * spaces
        constraints = f",\n{spaces}".join(sqls)
        return f"{spaces}{constraints}"

    def get_create_table_query(self, schema, name, table):
        columns = self.get_create_table_columns_sql(table)
        constraints = self.get_create_table_constraints_sql(table)
        sep = ""
        if constraints and columns:
            sep = ",\n"
        return (
            f'CREATE TABLE "{schema}"."{name}" (\n' f"{columns}{sep}{constraints})",
        )

    def get_column_sql(self, name, column):
        nullable = 'NULL' if column['null'] else 'NOT NULL'
        return f'"{name}" {column["type"]} {nullable}'

    def get_constraint_sql(self, name, constraint):
        columns = constraint["columns"]
        if columns:
            columns = ", ".join([f'"{c}"' for c in columns])
            columns = f" ({columns})"
        else:
            columns = ""
        related_name = constraint.get("related_name")
        related = ""
        if related_name:
            related_columns = ",\n  ".join(constraint["related_columns"])
            related = f" REFERENCES {related_name} ({related_columns})"
        return (
            f"CONSTRAINT {name} "
            f'{CONSTRAINT_TYPE_MAP[constraint["type"]]}{columns}{related}'
        )

    async def create_column(self, schema, table, name, column):
        await self.target.execute(
            *self.get_create_column_query(schema, table, name, column)
        )
        return True

    async def create_columns(self, columns, parents=None):
        return await self.create_table_items("column", columns, parents)

    async def create_constraint(self, schema, table, name, constraint):
        await self.target.execute(
            *self.get_create_constraint_query(schema, table, name, constraint)
        )
        return True

    async def create_constraints(self, constraints, parents=None):
        return await self.create_table_items("constraint", constraints, parents)

    async def create_index(self, schema, table, name, index):
        await self.target.execute(
            *self.get_create_index_query(schema, table, name, index)
        )
        return True

    async def create_indexes(self, indexes, parents=None):
        # ignore indexes with constraints
        # TODO: better way to identify all of these cases:
        # index + constraint: (u)nique, (p)rimary, (f)oreign, e(x)clusion
        # index-only: (c)heck
        return await self.create_table_items(
            "index",
            indexes,
            parents,
            exclude=lambda index: index["primary"] or index["unique"],
        )

    async def create_table_items(self, name, data, parents, exclude=None):
        assert len(parents) == 2
        schema, table = parents[0]
        for item_name, item in data.items():
            if exclude and exclude(item):
                continue
            await getattr(self, f"create_{name}")(schema, table, item_name, item)
        return data

    async def create_table(self, schema, name, table):
        await self.target.execute(*self.get_create_table_query(schema, name, table))
        return True

    async def create_tables(self, tables, parents=None):
        assert len(parents) == 1
        schema = parents[0]

        for table_name, table in tables.items():
            await self.create_table(schema, table_name, table)
        return tables

    async def create_schema(self, schema):
        await self.target.execute(*self.get_create_schema_query(schema))
        return True

    async def create_schemas(self, schemas, parents=None):
        for schema, tables in schemas.items():
            await self.create_schema(schema)
            await self.create_tables(tables, parents=[schema])
        return schemas

    async def drop_column(self, schema, table, name):
        if not self._drop_columns:
            return False
        query = self.get_drop_column_query(schema, table, name)
        await self.target.execute(*query)
        return True

    async def drop_columns(self, columns, parents=None):
        assert len(parents) == 2
        schema, table = parents

        if not self._drop_columns:
            return {}

        for name in columns.keys():
            await self.drop_column(schema, table, name)
        return columns

    async def drop_constraint(self, schema, table, name):
        if not self._drop_columns:
            return False

        query = self.get_drop_constraint_query(schema, table, name)
        await self.target.execute(*query)
        return True

    async def drop_constraints(self, data, parents=None):
        assert len(parents) == 2
        schema, table = parents

        if not self._drop_constraints:
            return {}

        for name in data.keys():
            await self.drop_constraint(schema, table, name)

        return data

    async def drop_index(self, schema, name):
        if not self._drop_indexes:
            return False

        query = self.get_drop_index_query(schema, name)
        await self.target.execute(*query)
        return True

    async def drop_indexes(self, data, parents=None):
        assert len(parents) == 2
        schema, table = parents

        if not self._drop_indexes:
            return {}

        for name in data.keys():
            await self.drop_index(schema, name)

        return data

    async def drop_table(self, schema, name):
        if not self._drop_tables:
            return False

        await self.target.execute(*self.get_drop_table_query(schema, name))
        return True

    async def drop_schema(self, schema):
        if not self._drop_schemas:
            return False

        await self.target.execute(*self.drop_schema_query(schema))
        return True

    async def drop_tables(self, tables, parents=None):
        assert len(parents) == 1
        schema = parents[0]

        if not self._drop_tables:
            return {}

        for table in tables.keys():
            await self.drop_table(schema, table)
        return tables

    async def drop_schemas(self, schemas, parents=None):
        if not self._drop_schemas:
            return {}

        for schema_name in schemas.keys():
            await self.drop_schema(schema_name)
        return schemas

    async def merge_constraint(self, name, diff, parents=None):
        print('merge constraint', name, diff)
        raise NotImplementedError()

    async def merge_index(self, column, diff, parents=None):
        raise NotImplementedError()

    async def merge_column(self, column, diff, parents=None):
        print('merge column', column, diff)
        raise NotImplementedError()

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

    async def copy_data(self, diff):
        return None  # TODO!!!

    async def copy_metadata(self, diff):
        return await self.merge(diff, "schema", [])

    async def merge(self, diff, level, parents=None, parallel=True):
        if not diff:
            # both schemas are identical
            return {}

        self.log(f'merge: {level} {".".join(parents)}')
        plural = f"{level}s" if level[-1] != "x" else f"{level}es"
        create_all = getattr(self, f"create_{plural}")
        drop_all = getattr(self, f"drop_{plural}")
        merge = getattr(self, f"merge_{level}", None)

        if isinstance(diff, list):
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


class InfoStep(WorkflowStep):
    def validate(self):
        self.only = self.config.get('only', None)
        self._validate("source", read=True)

    async def execute(self):
        return await self.source.get_info(only=self.only)


class DiffStep(WorkflowStep):
    def validate(self):
        self._validate("source", read=True)
        self._validate("target", read=True)
        self.translate = self.config.get("translate", None)

    async def execute(self):
        return await self.source.diff(self.target, translate=self.translate)
