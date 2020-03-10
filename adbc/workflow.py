from adbc.utils import is_dsn
from adbc.database import Database
from asyncio import gather
from jsondiff.symbols import delete, insert


class Workflow(object):
    def __init__(self, name, config, databases):
        self.name = name
        self.config = config
        self.databases = databases
        steps = config.get('steps', [])
        if not steps:
            raise ValueError(f'workflow "{name}" has no steps')
        self.steps = [WorkflowStep(self, step) for step in steps]

    async def execute(self):
        results = []
        for step in self.steps:
            results.append(await step.execute())
        return results


class WorkflowStep(object):
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
        self.config = config
        self.validate()

    def validate(self):
        raise NotImplementedError()

    async def execute(self):
        raise NotImplementedError()

    def validate_database_url(self, name):
        databases = self.workflow.databases
        if name not in databases:
            raise Exception(
                f'The provided name "{name}" is not defined in "databases"'
            )

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
            raise Exception(
                f'The provided name "{name}" is not defined in "databases"'
            )
        return databases[name]

    def _validate(self, name, read=False, write=False, alter=False):
        config = self.config
        datasource = config.get(name)
        if not datasource:
            raise Exception(f'"{name}" is required')

        url = self.validate_database_url(datasource)
        config = self.validate_database_config(datasource)
        database = self.validate_connection(
            name,
            url,
            config,
            read=read,
            write=write,
            alter=alter
        )
        setattr(self, name, database)

    def validate_connection(
        self,
        name,
        url,
        config,
        read=False,
        write=False,
        alter=False
    ):
        # TODO: validate read/write/alter permissions
        # for a faster / more proactive error message
        return Database(
            name=name,
            url=url,
            config=config
        )


class CopyStep(WorkflowStep):
    def validate(self):
        self._validate('source', read=True)
        self._validate('target', read=True, write=True, alter=True)

        self._translate = self.config.get('translate', None)
        self._drop_schemas = self.config.get('drop_schemas', False)
        self._drop_tables = self.config.get('drop_tables', False)
        self._drop_columns = self.config.get('drop_columns', True)

    async def execute(self):
        translate = self._translate
        source = self.source
        target = self.target
        initial_diff = await source.diff(target, translate)
        meta_changes = await self.copy_metadata(initial_diff)
        data_changes = await self.copy_data(initial_diff)
        final_diff = await source.diff(target, translate)
        return {
            'initial_diff': initial_diff,
            'meta_changes': meta_changes,
            'data_changes': data_changes,
            'final_diff': final_diff
        }

    def get_create_schema_query(self, schema_name):
        return (f'CREATE SCHEMA "{schema_name}"', )

    def get_drop_schema_query(self, schema_name, table_name):
        return (f'DROP SCHEMA "{schema_name}" CASCADE', )

    def get_drop_table_query(self, schema_name, table_name):
        return (f'DROP TABLE "{schema_name}"."{table_name}" CASCADE', )

    def get_create_table_column_sql(self, column):
        # TODO
        pass

    def get_create_table_constraint_sql(self, constraint):
        # TODO
        pass

    def get_create_table_columns_sql(self, table, spaces=2):
        if not table.columns:
            return ''

        columns = []
        for column_name, column in table.columns.items():
            columns.append(self.get_create_table_column_sql(column))

        spaces = ' ' * spaces
        columns = '\n{spaces}'.join(columns)
        return f'{spaces}{columns}\n'

    def get_create_table_constraints_sql(self, table, spaces=2):
        if not table.constraints:
            return ''

        constraints = []
        for constraint_name, constraint in table.constraints.items():
            constraints.append(self.get_create_table_constraint_sql(constraint))

        spaces = ' ' * spaces
        constraints = '\n{spaces}'.join(constraints)
        return f'{spaces}{constraints}\n'

    def get_create_table_query(self, schema_name, table_name, table):
        columns = self.get_create_table_columns_sql(table)
        constraints = self.get_create_table_constraints_sql(table)
        return [
            f'CREATE TABLE "{schema_name}"."{table_name}" (\n'
            f'{columns}{constraints})'
        ]

    async def create_schema(self, schema_name):
        await self.target.execute(*self.get_create_schema_query(schema_name))
        return True

    async def create_table(self, schema_name, table_name, table):
        await self.target.execute(
            *self.get_create_table_query(schema_name, table_name, table)
        )

    async def drop_schema(self, schema_name):
        if not self._drop_schemas:
            return False

        await self.target.execute(*self.drop_schema_query(schema_name))
        return True

    async def drop_table(self, schema_name, table_name, table):
        if not self._drop_tables:
            return False

        await self.target.execute(*self.get_drop_table_query(schema_name, table_name))
        return True

    async def create_schemas(self, schemas, parents=None):
        for schema_name, tables in schemas.items():
            await self.create_schema(schema_name)
            await self.create_tables(tables, parents=[schema_name])
        return schemas

    async def create_columns(self, columns, parents=None):
        pass

    async def create_constraints(self, constraints, parents=None):
        pass

    async def create_tables(self, tables, parents=None):
        assert(len(parents) == 1)
        schema_name = parents[0]

        for table_name, table in tables.items():
            await self.create_table(schema_name, table_name, table)
        return tables

    async def drop_constraints(self, constraints, parents=None):
        assert(len(parents) == 2)
        schema_name, table_name = parents

        if not self._drop_constraints:
            return {}

        for constraint_name in constraints.keys():
            await self.drop_constraint(schema_name, table_name, constraint_name)
        return constraints

    async def drop_columns(self, columns, parents=None):
        assert(len(parents) == 2)
        schema_name, table_name = parents

        if not self._drop_columns:
            return {}

        for column_name in columns.keys():
            await self.drop_column(schema_name, table_name, column_name)
        return columns

    async def drop_tables(self, tables, parents=None):
        assert(len(parents) == 1)
        schema_name = parents[0]

        if not self._drop_tables:
            return {}

        for table_name in tables.keys():
            await self.drop_table(schema_name, table_name)
        return tables

    async def drop_schemas(self, schemas, parents=None):
        if not self._drop_schemas:
            return {}

        for schema_name in schemas.keys():
            await self.drop_schema(schema_name)
        return schemas

    async def drop_constraint(self, schema, table, constraint_name):
        pass

    async def add_constraint(self, schema, table, constraint_name, constraint):
        pass

    async def copy_table(self, table_name, diff, parents=None):
        parents = parents + [table_name]
        routines = [
            await self._copy(
                diff=diff[child],
                level=child,
                parents=parents
            ) for child in ('columns', 'constraints', 'indexes')
            if diff.get(child)
        ]
        return await gather(*routines)

    async def copy_schema(self, schema_name, diff, parents=None):
        return await self._copy(
            diff=diff,
            level='table',
            parents=parents + [schema_name]
        )

    async def copy_metadata(self, diff):
        return await self._copy(
            diff=diff,
            level='schema',
            parents=[]
        )

    async def _copy(self, diff=None, level=None, parents=None):
        if not diff:
            # both schemas are identical
            return {}

        create_all = getattr(self, f'create_{level}s')
        drop_all = getattr(self, f'drop_{level}s')
        copy = getattr(self, f'copy_{level}', None)

        if isinstance(diff, list):
            assert(len(diff) == 2)
            # source and target have no overlap
            # -> copy by dropping all in target not in source
            # and creating all in source not in target
            source, target = diff
            # do these two actions in parallel
            inserted, deleted = await gather(
                create_all(source, parents=parents),
                drop_all(source, parents=parents)
            )
            return {
                insert: inserted,
                delete: deleted
            }
        else:
            assert(isinstance(diff, dict))
            routines = []
            names = []
            for name, changes in diff.items():
                action = None
                if name == delete:
                    action = drop_all(changes, parents=parents)
                elif name == insert:
                    action = create_all(changes, parents=parents)
                elif copy:
                    action = copy(name, changes, parents=parents)

                if action:
                    routines.append(action)
                    names.append(name)

            results = await gather(*routines)
            return {r[0]: r[1] for r in zip(names, results)}


class InfoStep(WorkflowStep):
    def validate(self):
        self._validate('source', read=True)

    async def execute(self):
        return await self.source.get_diff_data()


class DiffStep(WorkflowStep):
    def validate(self):
        self._validate('source', read=True)
        self._validate('target', read=True)
        self.translate = self.config.get('translate', None)

    async def execute(self):
        return await self.source_database.diff(self.target, translate=self.translate)
