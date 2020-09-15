import io
from math import ceil
from asyncio import gather
from jsondiff.symbols import insert, delete
from adbc.sql import print_query
from adbc.utils import AsyncBuffer, aecho, confirm
from adbc.constants import SEP, SEPN
from adbc.preql import build
from .merge import WithMerge
from .drop import WithDrop
from .create import WithCreate
from .diff import WithDiff


class WithCopy(WithMerge, WithDrop, WithCreate, WithDiff):
    parallel_copy = True

    async def _copy_shard(
        self,
        source_model,
        target_model,
        target,
        pk,
        source_schema,
        source_table,
        target_schema,
        target_table,
        delete,
        truncate,
        cursor_min,
        cursor_max,
    ):
        # md5 check failed
        # copy this shard

        # maybe use transaction for delete?
        # if there are no FK constraints, it shouldn't block
        # but we would still get a more consistent table state
        # for example, row counts will not change as large chunks of rows
        # are deleted and re-added in a transaction

        connection = None  # await self.get_connection() if delete else None
        transaction = aecho()  # connection.transaction() if delete else aecho()

        def get_query(q):
            columns = q.table.order_by_alias(
                q.table.columns.keys()
            )
            q = q.take(*columns)
            if cursor_min:
                q = q.where({
                    "and": [
                        {'>': [pk, cursor_min]},
                        {'<=': [pk, cursor_max]}
                    ]
                })
            else:
                q = q.where({'<=': [pk, cursor_max]})
            return q

        try:
            async with transaction:
                source_query = get_query(source_model)
                if delete:
                    if truncate:
                        await target_model.truncate()
                    else:
                        target_query = get_query(target_model)
                        await target_query.delete(connection=connection)

                target_columns = target_model.table.order_by_alias(
                    target_model.table.columns.keys()
                )
                # copy from source to buffer
                source_query = await source_query.get(preql=True)
                if self.parallel_copy:
                    buffer = AsyncBuffer()
                    copy_from, copy_to = await gather(
                        self.copy_from(
                            query=source_query,
                            output=buffer.write,
                            close=buffer
                        ),
                        target.copy_to(
                            table_name=target_table,
                            schema_name=target_schema,
                            source=buffer,
                            connection=connection,
                            columns=target_columns,
                        ),
                    )
                    return copy_to
                else:
                    buffer = io.BytesIO()
                    await self.copy_from(query=source_query, output=buffer)
                    buffer.seek(0)
                    return await target.copy_to(
                        table_name=target_table,
                        schema_name=target_schema,
                        source=buffer,
                        columns=target_columns,
                        connection=connection,
                    )
        finally:
            if connection:
                await connection.close()

    async def _copy_table(
        self,
        target,
        source_schema,
        source_table,
        source_metadata,
        target_schema,
        target_table,
        target_metadata,
        scope
    ):
        source_model = await self.get_model(source_table, schema=source_schema, scope=scope)
        target_model = await target.get_model(target_table, schema=target_schema, scope=scope)
        schema = source_metadata  # ["schema"]
        columns = schema["columns"]
        column_names = list(sorted(columns.keys()))
        indexes = schema.get("indexes", {})
        constraints = schema.get("constraints", {})
        pks = source_model.table.pks  #  get_pks(indexes, constraints, column_names)
        source_count = source_metadata["rows"]["count"]

        source_max = await self.shard_size
        target_max = await target.shard_size
        max_size = min(source_max, target_max)
        num_shards = 1
        pk = None
        if len(pks) == 1:
            pk = next(iter(pks.keys()))

        if source_count > max_size and pk:
            num_shards = ceil(source_count / max_size)

        shards_label = ""
        if num_shards > 1:
            shards_label = f" ({num_shards})"

        self.log(f"copy (start): {source_schema}.{source_table}{shards_label}")

        cursor = None
        num_shards = num_shards if pk else 1
        single = num_shards == 1
        max_size = max_size if num_shards > 1 else None
        last = num_shards - 1
        source_low = source_high = target_high = None
        target_rows = target_metadata['rows']
        source_rows = source_metadata['rows']
        if pk and target_rows["range"]:
            # pk should be here
            target_range = target_rows["range"][pk]
            source_range = source_rows["range"][pk]
            source_low = source_range["min"]
            source_high = source_range["max"]
            target_high = target_range["max"]

        skipped = 0
        copiers = []
        for shard in range(num_shards):

            # if there is a pk, we are using keyset pagination
            # only delete rows not within the bounds of the source data
            if pk and shard == 0 and source_low:
                # drop any target rows with id before the lowest source ID
                await target_model.where({'<': [pk, source_low]}).delete()

            if pk and (target_high is None or cursor and cursor > target_high):
                # skip the check and move on to delete/copy
                # if the cursor is beyond the highest target ID

                # we still need to get the next ID
                # but we do not need the md5
                source_check = source_model.table.get_statistics(
                    max_pk=True, cursor=cursor, limit=max_size,
                )
                source_result = await source_check
                target_count = 0
                source_max = source_result["max"]
            else:
                source_check = source_model.table.get_statistics(
                    md5=True,
                    min_pk=True,
                    count=True,
                    max_pk=True,
                    cursor=cursor,
                    limit=max_size,
                )
                target_check = target_model.table.get_statistics(
                    md5=True,
                    min_pk=True,
                    count=True,
                    max_pk=True,
                    cursor=cursor,
                    limit=max_size,
                )
                source_result, target_result = await gather(source_check, target_check)

                source_md5 = source_result["md5"]
                source_count = source_result["count"]
                source_min = source_result["min"]
                source_max = source_result["max"]

                target_md5 = target_result["md5"]
                target_count = target_result["count"]
                target_min = target_result["min"]
                target_max = target_result["max"]

                if (
                    source_md5 == target_md5
                    and source_count == target_count
                    and source_min == target_min
                    and source_max == target_max
                ):
                    # md5 check pass
                    # set cursor -> source max
                    cursor = source_max
                    skipped += source_count
                    continue

            delete = target_count > 0
            copiers.append(
                self._copy_shard(
                    source_model,
                    target_model,
                    target,
                    pk,
                    source_schema,
                    source_table,
                    target_schema,
                    target_table,
                    delete,
                    single,
                    cursor,
                    source_max,
                )
            )

            if not single and pk and shard == last:
                # drop after the last shard
                await target_model.where({'>': [pk, source_high]}).delete()

            cursor = source_max

        copied = sum(await gather(*copiers)) if copiers else 0
        return {"copied": copied, "skipped": skipped}

    async def copy_metadata(self, diff, scope=None):
        return await self.merge(diff, "schema", [], scope=scope)

    async def copy_data(
        self, target, source_info, target_info, diff, scope=None, check_all=False,
    ):
        keys = []
        values = []
        to_source = self.get_scope_translation(scope=scope, to="source")
        to_target = self.get_scope_translation(scope=scope, to="target")
        if check_all:
            # check all tables
            for schema_name, tables in target_info.items():
                source_schema = to_source.get(schema_name, schema_name)
                target_schema = to_target.get(schema_name, schema_name)

                schema_scope = self.get_child_scope(source_schema, scope=scope)
                to_source_table = self.get_scope_translation(
                    scope=schema_scope, to='source', child_key='tables'
                )
                to_target_table = self.get_scope_translation(
                    scope=schema_scope, to='target', child_key='tables'
                )

                for table_name, table in tables.items():

                    source_metadata = source_info[schema_name][table_name]
                    target_metadata = target_info[schema_name][table_name]
                    source_table = to_source_table.get(table_name, table_name)
                    target_table = to_target_table.get(table_name, table_name)

                    keys.append((target_schema, target_table))
                    values.append(
                        self._copy_table(
                            target,
                            source_schema,
                            source_table,
                            source_metadata,
                            target_schema,
                            target_table,
                            target_metadata,
                            scope
                        )
                    )
        else:
            if not diff:
                return {}
            # only look over diffed tables
            # this is usually sufficient, unless a tables content has changed
            # but min id/max id/count have not changed
            # this can happen for tables with frequent updates

            # if a table is immutable, however, this is reliable
            # and will be more efficient as it will avoid checking hashes
            # on large tables without count mismatches
            for schema_name, schema_changes in diff.items():
                if schema_name == delete or schema_name == insert:
                    continue

                schema_scope = self.get_child_scope(schema_name, scope=scope)
                to_source_table = self.get_scope_translation(
                    scope=schema_scope, to='source', child_key='tables'
                )
                to_target_table = self.get_scope_translation(
                    scope=schema_scope, to='target', child_key='tables'
                )

                for table_name, changes in schema_changes.items():
                    if "rows" in changes:

                        source_metadata = source_info[schema_name][table_name]
                        target_metadata = target_info[schema_name][table_name]

                        source_schema = to_source.get(schema_name, schema_name)
                        target_schema = to_target.get(schema_name, schema_name)

                        source_table = to_source_table.get(table_name, table_name)
                        target_table = to_target_table.get(table_name, table_name)

                        keys.append((target_schema, target_table))
                        values.append(
                            self._copy_table(
                                target,
                                source_schema,
                                source_table,
                                source_metadata,
                                target_schema,
                                target_table,
                                target_metadata,
                            )
                        )

        if values:
            values = await gather(*values)

        result = {}
        for i, (schema, table) in enumerate(keys):
            if schema not in result:
                result[schema] = {}
            result[schema][table] = values[i]
        return result

    async def copy(
        self, target, scope=None, check_all=True, final_diff=True, exclude=None
    ):
        schema_diff = await self.diff(
            target,
            scope=scope,
            data=False,
            exclude=exclude
        )

        schema_changes = await target.copy_metadata(
            schema_diff,
            scope=scope
        )

        if schema_changes:
            # reset target schema
            target.reset()

        source_info, target_info, data_diff = await self.diff(
            target, scope=scope, info=True, exclude=exclude
        )
        data_changes = await self.copy_data(
            target,
            source_info,
            target_info,
            data_diff,
            scope=scope,
            check_all=check_all,
        )
        # TODO: drop and add the FK constraints before/after copy_data
        if final_diff:
            final_diff = await self.diff(target, scope=scope, exclude=exclude)
        return {
            "schema_changes": schema_changes,
            "data_changes": data_changes,
            "final_diff": final_diff,
        }

    async def copy_from(self, **kwargs):
        pool = await self.pool
        table_name = kwargs.pop("table_name", None)
        schema_name = kwargs.get('schema_name', None)
        transaction = kwargs.pop("transaction", False)
        connection = kwargs.pop("connection", self._connection)
        connection = aecho(connection) if connection else pool.acquire()
        close = kwargs.pop("close", False)
        query = kwargs.pop("query", None)
        params = kwargs.pop('params', None)

        if table_name:
            target_label = f"{schema_name}.{table_name}" if schema_name else table_name
        else:
            if not query:
                raise NotImplementedError("table or query is required")
            if isinstance(query, (list, dict)):
                # compile from PreQL
                query, params = build(query, dialect=self.backend.dialect, combine=True)

            target_label = print_query(query, params)


        if self.prompt:
            if not confirm(f"{self.name} ({self.tag}): {SEP}copy from {target_label}{SEPN}", True):
                raise Exception(f"{self}: copy_from aborted")
        else:
            self.log(f"{self}: copy_from{SEP}{target_label}{SEPN}")

        async with connection as conn:
            transaction = conn.transaction() if transaction else aecho()
            async with transaction:
                result = None
                if table_name:
                    result = await self.backend.copy_from_table(conn, table_name, **kwargs)
                else:
                    result = await self.backend.copy_from_query(conn, query, params, **kwargs)
                if close:
                    if hasattr(close, "close"):
                        # close passed in object
                        output = close
                    else:
                        # close output object
                        output = kwargs.get("output")

                    if getattr(output, "close"):
                        output.close()
                return result

    async def copy_to(self, **kwargs):
        pool = await self.pool
        table_name = kwargs.pop("table_name", None)
        transaction = kwargs.pop("transaction", False)
        schema_name = kwargs.get('schema_name', None)
        columns = ', '.join(kwargs.get('columns', []))
        columns = f' ({columns})' if columns else ''
        target_label = f"{schema_name}.{table_name}{columns}" if schema_name else table_name
        connection = kwargs.pop("connection", None) or self._connection
        connection = aecho(connection) if connection else pool.acquire()

        if self.prompt:
            if not confirm(f"{self.name} ({self.tag}): {SEP}copy to {target_label}{SEPN}", True):
                raise Exception(f"{self}: copy_to aborted")
        else:
            self.log(f"{self}: copy_to{SEP}{target_label}{SEPN}")

        async with connection as conn:
            transaction = conn.transaction() if transaction else aecho()
            async with transaction:
                return await self.backend.copy_to_table(conn, table_name, **kwargs)
