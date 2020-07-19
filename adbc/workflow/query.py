from .step import Step
import sys
from adbc.constants import QUERY_SPLIT_SIZE


class QueryStep(Step):
    def validate(self):
        self._validate("source")
        self.query = self.config.get('query', None)
        self.fetch = self.config.get('fetch', False)
        self.split = self.config.get('split', {})

    async def execute(self):
        fetch = self.fetch
        if fetch == 'one':
            method = 'query_one_row'
        elif fetch:
            method = 'query'
        else:
            method = 'execute'

        split = self.split
        base_query = self.query
        if split:
            # TODO: remove the hacks and make this more
            # more generally
            if isinstance(split, str):
                on = split
                size = QUERY_SPLIT_SIZE
            else:
                on = split['on']
                size = split.get('size', QUERY_SPLIT_SIZE)
            # assumption: split field is given with the original table name
            # so that we can look up the table with get_model
            # also assume that we can only split on integer IDs for now
            # datetime and sequential string IDs should also work in theory
            dots = on.count('.')
            if dots == 2:
                schema, table, column = on.split('.')
            elif dots == 1:
                schema = None
                table, column = on.split('.')
            else:
                raise ValueError('must pass [schema.]table.column to split.on')

            model = await self.source.get_model(table, schema=schema)
            data_range = await model.table.get_data_range([column])
            data_min = data_range[column]['min']
            data_max = data_range[column]['max']

            cursor = data_min
            results = []
            shard = 0
            shards = int(round((data_max - data_min) / size, 0))
            while cursor <= data_max:
                # TODO: this is a big hack, would be easier with PreQL
                # to do this properly in SQL requires a full SQL parser
                query = f'{base_query} AND {on} >= $1 AND {on} < $2'
                query = (query, cursor, min(cursor + size, data_max + 1))
                result = await getattr(self.source, method)(*query)
                results.append(result)
                shard += 1
                if self.verbose:
                    sys.stdout.write(f'query: finished shard {shard} of {shards}    \r')
                    sys.stdout.flush()
                cursor += size
            return results
        else:
            result = await getattr(self.source, method)(base_query)
            self.log(f'query: finished {self.query}')
            return result
