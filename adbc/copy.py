import io
from math import ceil
from asyncio import gather
from jsondiff.symbols import insert, delete
from .sql import get_pks, can_order
from .utils import AsyncContext, AsyncBuffer
from .merge import WithMerge
from .drop import WithDrop
from .create import WithCreate
from .diff import WithDiff


class WithCopy(WithMerge, WithDrop, WithCreate, WithDiff):
    parallel_copy = True

    async def get_max_copy_size(self):
        is_redshift = await self.is_redshift
        return 1000 if is_redshift else 16000

    async def _delete_before(self, pk, schema, table, value):
        return await self._delete_edge(pk, schema, table, value)

    async def _delete_after(self, pk, schema, table, value):
        return await self._delete_edge(pk, schema, table, value, before=False)

    async def _delete_edge(self, pk, schema, table, value, before=True):
        model = await self.model(schema, table)
        operator = '<' if before else '>'
        return await model.where({pk: {operator: value}}).delete()

    async def _check_shard(
        self,
        db,
        schema,
        table_name,
        md5=True,
        min_pk=True,
        max_pk=True,
        count=True,
        cursor=None,
        limit=None
    ):
        model = await db.model(schema, table_name)
        table = model.table
        kwargs = {
            'cursor': cursor,
            'limit': limit,
            'count': count,
            'md5': md5,
            'max_pk': max_pk,
            'min_pk': min_pk
        }
        split = False
        if table.pks:
            pk = table.pks[0]
            split = table.columns[pk]['type'] == 'uuid'

        if not split:
            return await table.get_statistics(**kwargs)
        else:
            # split query
            kwargs['min_pk'] = False
            kwargs['max_pk'] = False

            if md5 or count:
                md5 = table.get_statistics(**kwargs)
            else:
                md5 = False
            if min_pk:
                min_pk = table.get_min_id(cursor=cursor, limit=limit)
            if max_pk:
                max_pk = table.get_max_id(cursor=cursor, limit=limit)

            tasks = []
            if md5:
                tasks.append(md5)
            if min_pk:
                tasks.append(min_pk)
            if max_pk:
                tasks.append(max_pk)

            results = await gather(*tasks)
            i = 0

            if md5:
                md5 = results[i]
                i += 1
            if min_pk:
                min_pk = results[i]
                i += 1

            if max_pk:
                max_pk = results[i]
                i += 1

            result = {}
            if md5:
                result.update(md5.items())
            if min_pk:
                result['min'] = min_pk
            if max_pk:
                result['max'] = max_pk
            return result

    async def _copy_shard(
        self,
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

        connection = None  # await self.target.get_connection() if delete else None
        transaction = AsyncContext()  # connection.transaction() if delete else aecho()
        try:
            async with transaction:
                query = await target.model(target_schema, target_table)
                if not truncate:
                    if cursor_min:
                        query = query.where({
                            ".and": [
                                {pk: {">": cursor_min}},
                                {pk: {"<=": cursor_max}},
                            ]
                        })
                    elif cursor_max:
                        query = query.where({pk: {"<=": cursor_max}})

                if delete:
                    if truncate:
                        await self._truncate(target_schema, target_table)
                    else:
                        await query.delete(connection=connection)

                # copy from source to buffer
                sql = await query.get(sql=True)
                if self.parallel_copy:
                    buffer = AsyncBuffer()
                    copy_from, copy_to = await gather(
                        self.source.copy_from(
                            query=sql,
                            output=buffer.write,
                            close=buffer
                        ),
                        target.copy_to(
                            table_name=target_table,
                            schema_name=target_schema,
                            source=buffer,
                            connection=connection,
                        )
                    )
                    return copy_to
                else:
                    buffer = io.BytesIO()
                    await self.source.copy_from(query=sql, output=buffer)
                    buffer.seek(0)
                    return await target.copy_to(
                        table_name=target_table,
                        schema_name=target_schema,
                        source=buffer,
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
    ):
        schema = source_metadata["schema"]
        columns = schema["columns"]
        column_names = list(sorted(columns.keys()))
        indexes = schema.get("indexes", {})
        constraints = schema.get("constraints", {})
        pks = get_pks(indexes, constraints, column_names)
        source_count = source_metadata["data"]["count"]

        source_max = await self.get_max_copy_size()
        target_max = await target.get_max_copy_size()
        max_size = min(source_max, target_max)
        num_shards = 1
        pk = None
        if len(pks) == 1:
            pk = pks[0]

        if source_count > max_size and pk:
            if can_order(columns[pk]["type"]):
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
        if pk and target_metadata['data']['range']:
            # pk should be here
            target_range = target_metadata['data']['range'][pk]
            source_range = source_metadata['data']['range'][pk]
            source_low = source_range['min']
            source_high = source_range['max']
            target_high = target_range['max']

        skipped = 0
        copiers = []
        for shard in range(num_shards):

            # if there is a pk, we are using keyset pagination
            # only delete rows not within the bounds of the source data
            if pk and shard == 0 and source_low:
                # drop any target rows with id before the lowest source ID
                await self._delete_before(
                    pk, target_schema, target_table, source_low
                )

            if pk and (target_high is None or cursor and cursor > target_high):
                # skip the check and move on to delete/copy
                # if the cursor is beyond the highest target ID

                # we still need to get the next ID
                # but we do not need the md5
                source_check = self._check_shard(
                    self.source, source_schema, source_table,
                    md5=False,
                    min_pk=False,
                    count=False,
                    cursor=cursor,
                    limit=max_size
                )
                source_result = await source_check
                target_count = 0
                source_max = source_result['max']
            else:
                source_check = self._check_shard(
                    self.source, source_schema, source_table,
                    cursor=cursor, limit=max_size
                )
                target_check = self._check_shard(
                    target, target_schema, target_table,
                    cursor=cursor, limit=max_size
                )
                source_result, target_result = await gather(source_check, target_check)

                source_md5 = source_result['md5']
                source_count = source_result['count']
                source_min = source_result['min']
                source_max = source_result['max']

                target_md5 = target_result['md5']
                target_count = target_result['count']
                target_min = target_result['min']
                target_max = target_result['max']

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
                else:
                    if self.verbose:
                        print(f'shard mismatch: {target_schema}.{target_table}#{shard}')

            delete = target_count > 0
            copiers.append(self._copy_shard(
                target,
                pk,
                source_schema,
                source_table,
                target_schema,
                target_table,
                delete,
                single,
                cursor,
                source_max
            ))

            if not single and pk and shard == last:
                # drop after the last shard
                await self._delete_after(
                    pk, target_schema, target_table, source_high
                )

            cursor = source_max

        copied = sum(await gather(*copiers)) if copiers else 0
        return {'copied': copied, 'skipped': skipped}

    async def copy_metadata(self, diff):
        return await self.merge(diff, "schema", [])

    async def copy_data(self, target, source_info, target_info, diff, translate=None):
        if not diff:
            return {}

        source_schemas = (
            {v: k for k, v in translate.get("schemas", {}).items()} if translate else {}
        )
        keys = []
        values = []
        if self.check_all:
            # check all tables
            for target_schema, target_tables in target.items():
                source_schema = source_schemas.get(target_schema, target_schema)
                for target_table, table_schema in target_tables.items():
                    source_table = target_table
                    keys.append(target_table)
                    source_metadata = source_info[source_schema][source_table]
                    target_metadata = target_info[target_schema][target_table]
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
        else:
            # only look over diffed tables
            # this is usually sufficient, unless a tables content has changed
            # but min id/max id/count have not changed
            # this can happen for e.g. an updating session table
            for target_schema, schema_changes in diff.items():
                if target_schema == delete or target_schema == insert:
                    continue

                source_schema = source_schemas.get(target_schema, target_schema)
                for target_table, changes in schema_changes.items():
                    source_table = target_table
                    if "data" in changes:
                        keys.append(target_table)
                        source_metadata = source_info[source_schema][source_table]
                        target_metadata = target_info[target_schema][target_table]
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
        return dict(zip(keys, values))

    def copy(self, target, translate=None):
        schema_diff = await self.diff(
            target, translate=translate, only="schema", info=False
        )
        schema_changes = await self.copy_metadata(schema_diff)
        source_info, target_info, data_diff = await self.diff(
            target, translate=translate, info=True, refresh=True
        )
        data_changes = await self.copy_data(
            target, source_info, target_info, data_diff, translate=translate
        )
        final_diff = await self.diff(target, translate, refresh=True)
        return {
            'schema_diff': schema_diff,
            'schema_changes': schema_changes,
            'data_changes': data_changes,
            'final_diff': final_diff
        }
