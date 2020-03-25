import os
import uuid
import io
from copy import copy
from math import ceil
from aiobotocore import get_session
from adbc.utils import is_dsn
from adbc.table import Table, can_order
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
            debug = False
            if command.startswith('?'):
                debug = True
                command = command[1:]
            if command == "copy":
                step = CopyStep(workflow, config)
            elif command == "diff":
                step = DiffStep(workflow, config)
            elif command == "info":
                step = InfoStep(workflow, config)
            else:
                raise Exception(f'the provided command "{command}" is not supported')
            if debug:
                return DebugStep(step)
            else:
                return step
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


# TODO: implement mode: full upsert:
# this is possible if there are primary keys
# in this case, we create a temporary table to copy into
# then perform UPSERT rows from the temporary table
# into the original table, then remove the temp table

# TODO: implement mode: delta immutable
# if the table has a created field and is immutable
# and the created field min matches between source and target
# and the target created max is lower than the source
# in that case, take the diff of maxes, selectively COPY
# and insert that data, then check the data diff again
# if there is a mismatch at any step, fallback to full copy
# or full upsert

# TODO: implement mode: delta
# if the table has an updated field
# and the updated field min matches between source and target
# and the target updated max is lower
# in that case, take the diff of maxes, selectively COPY
# and insert that data, then check data diff
# if there is a mismatch, fallback to full upsert

# TODO: implement chunked copy
# if there are more than 500K rows, operations can get very slow
# it is possible to chunk copy or select operation in any mode
# as long as the table has a sortable key
# like an integer or varchar pk, created, or updated field (not UUID)

class DebugStep(object):
    """debug another step"""
    def __init__(self, step):
        self.step = step

    async def execute(self):
        print('PAUSING BEFORE EXECUTE')
        import pdb
        pdb.set_trace()

        value = await self.step.execute()

        print('PAUSING AFTER EXECUTE')
        import pdb
        pdb.set_trace()
        return value


class CopyStep(WorkflowStep):
    def validate(self):
        # unique prefix for this job
        self.prefix = str(uuid.uuid4()) + "/"
        self._validate("source", read=True)
        self._validate("target", read=True, write=True, alter=True)

        self.translate = self.config.get("translate", None)
        drop_all = self.config.get("drop", False)
        self._drop_schemas = self.config.get("drop_schemas", False) or drop_all
        self._drop_tables = self.config.get("drop_tables", False) or drop_all
        self._drop_columns = self.config.get("drop_columns", True) or drop_all
        self._drop_constraints = self.config.get("drop_constraints", True) or drop_all
        self._drop_indexes = self.config.get("drop_indexes", True) or drop_all

    async def _validate_s3(self):
        target_version = await self.target.version
        if target_version < 9:
            # must have S3 credentials
            bucket = self.target.config.get("aws_s3_bucket")
            region = self.target.config.get("aws_s3_region")
            secret = self.target.config.get("aws_secret_access_key") or os.environ.get(
                "AWS_SECRET_ACCESS_KEY"
            )
            access_key = self.target.config.get("aws_access_key_id") or os.environ.get(
                "AWS_ACCESS_KEY_ID"
            )
            prefix = self.target.config.get("aws_s3_prefix") or ""
            assert bucket
            assert secret
            assert region
            assert access_key
            assert secret

            self.use_s3 = True
            self.s3_access_key = access_key
            self.s3_secret = secret
            self.s3_region = region
            self.s3_bucket = bucket
            self.s3_prefix = prefix or ""
            session = get_session()
            self.s3 = session.create_client(
                "s3",
                region_name=region,
                aws_secret_access_key=secret,
                aws_access_key_id=access_key,
            )
        else:
            self.use_s3 = None

    async def _validate_source(self):
        source_version = await self.source.version
        self.use_select = source_version < 9

    async def execute(self):
        translate = self.translate
        source = self.source
        target = self.target
        schema_diff = await source.diff(
            target, translate=translate, only="schema", info=False
        )
        meta_changes = await self.copy_metadata(schema_diff)
        source_info, target_info, data_diff = await source.diff(
            target, translate=translate, info=True
        )
        # may be required to use S3
        await self._validate_source()
        await self._validate_s3()
        data_changes = await self.copy_data(
            source_info, target_info, data_diff, translate=translate
        )
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
        nullable = "NULL" if column["null"] else "NOT NULL"
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
        print("merge constraint", name, diff)
        raise NotImplementedError()

    async def merge_index(self, column, diff, parents=None):
        raise NotImplementedError()

    async def merge_column(self, column, diff, parents=None):
        print("merge column", column, diff)
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

    async def copy_data(self, source, target, diff, translate=None):
        if not diff:
            return {}

        source_schemas = (
            {v: k for k, v in translate.get("schemas", {}).items()} if translate else {}
        )
        keys = []
        values = []
        for target_schema, schema_changes in diff.items():
            if target_schema == delete or target_schema == insert:
                continue

            source_schema = source_schemas.get(target_schema, target_schema)
            for target_table, changes in schema_changes.items():
                source_table = target_table
                if "data" in changes:
                    keys.append(target_table)
                    source_metadata = source[source_schema][source_table]
                    target_metadata = target[target_schema][target_table]
                    values.append(
                        self.copy_table(
                            source_schema,
                            source_table,
                            source_metadata,
                            target_schema,
                            target_table,
                            target_metadata,
                            changes["data"],
                        )
                    )
                if "schema" in changes:
                    raise Exception(
                        f"schema changed during copy: "
                        f"{source_schema}.{source_table}: {changes['schema']}"
                    )

        if values:
            values = await gather(*values)
        return dict(zip(keys, values))

    async def copy_table(
        self,
        source_schema,
        source_table,
        source_metadata,
        target_schema,
        target_table,
        target_metadata,
        diff,
    ):
        # full copy
        # 1) * -> postgres (use COPY -> COPY)
        # 2) * -> redshift (use COPY -> S3 <- COPY)
        method = self._copy_table_s3 if self.use_s3 else self._copy_table
        return await method(
            source_schema,
            source_table,
            source_metadata,
            target_schema,
            target_table,
            target_metadata,
            diff,
        )

    def get_max_copy_size(self):
        return 10000

    async def _copy_shard_s3(
        self,
        source_schema,
        source_table,
        target_schema,
        target_table,
        shard,
        num_shards,
        pk=None,
        max_size=None,
        cursor=None,
        columns=None,
        bucket=None,
        key=None,
    ):
        output = io.BytesIO()
        # 1. compareshard count, cursor, and md5 across source and target
        # if they all match, do not copy anything

        await self.source.copy_from(output=output, format="csv", query=query)
        self.log(
            f"copy (out): {source_schema}.{source_table} "
            f"({shard+1} of {num_shards})"
        )
        output = output.getvalue()

        await self.s3.put_object(
            Bucket=bucket,
            Key=key,
            Body=output
        )
        self.log(
            f"copy (s3): {source_schema}.{source_table} "
            f"({shard+1} of {num_shards})"
        )

        # 2. delete all rows in the shard from the target
        # 3. copy rows in
        try:
            # actually perform the copy
            num_copied = await self.target.execute(
                f'COPY "{target_schema}"."{target_table}"\n'
                f"({cols})\n"
                f"FROM '{s3_url}'\n"
                f"FORMAT CSV\n"
                f"SECRET_ACCESS_KEY '{self.s3_secret}'\n"
                f"ACCESS_KEY_ID '{self.s3_access_key}'\n"
                f"COMPUPDATE OFF STATUPDATE OFF"
            )
        finally:
            await self.s3.delete_object(Key=key, Bucket=bucket)

        num_copied = int(num_copied.replace('COPY ', ''))
        return cursor, num_copied

    async def _copy_table_s3(
        self,
        source_schema,
        source_table,
        source_metadata,
        target_schema,
        target_table,
        target_metadata,
        diff,
    ):
        schema = source_metadata["schema"]
        columns = schema["columns"]
        column_names = list(sorted(columns.keys()))
        indexes = schema.get("indexes", {})
        constraints = schema.get("constraints", {})
        pks = Table.get_pks(indexes, constraints, column_names)

        count = source_metadata["data"]["count"]
        max_size = self.get_max_copy_size()
        num_shards = 1
        pk = None
        cursor = None
        if count > max_size and len(pks) == 1:
            pk = pks[0]
            if can_order(columns[pk]['type']):
                # if the table is large enough
                # and there is a single PK that supports ordering (not UUID)
                # and the PK range is captured in source data,
                # split the copy into shards
                num_shards = ceil(count / max_size)

        bucket = self.s3_bucket
        s3_folder = f"{self.s3_prefix}{self.prefix}{source_schema}.{source_table}/"
        shards_label = ''
        if num_shards > 1:
            shards_label = f' ({num_shards})'
        self.log(f"copy (start): {source_schema}.{source_table}{shards_label}")
        shard = 0
        try:
            for shard in range(num_shards):
                s3_key = f"{s3_folder}{shard}.csv"
                cursor, num_copied = await self._copy_shard_s3(
                    source_schema,
                    source_table,
                    target_schema,
                    target_table,
                    shard,
                    num_shards,
                    pk=pk,
                    max_size=max_size,
                    cursor=cursor,
                    columns=column_names,
                    bucket=bucket,
                    key=s3_key,
                )

        # remove keys
        # finally does not work here because of async quirks
        except Exception:
            self.log(
                f"copy (fail): {source_schema}.{source_table} "
                f"({shard} of {num_shards})"
            )
            raise
        else:
            self.log(
                f"copy (in): {source_schema}.{source_table}{shards_label}"
            )

    async def copy_metadata(self, diff):
        return await self.merge(diff, "schema", [])

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


class InfoStep(WorkflowStep):
    def validate(self):
        self.only = self.config.get("only", None)
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
