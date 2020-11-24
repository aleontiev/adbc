from adbc.zql import literal
import re


executors = {}
def get_executor(source, scope=None):
    global executors
    key = (source.name, source.url, scope)
    if key not in executors:
        executors[key] = QueryExecutor(source, scope=scope)
    return executors[key]


class QueryExecutor(object):
    def __init__(self, database, scope=None):
        self.database = database
        self.scope = scope

    def all_columns(self, table, level=None):
        return list(table.columns.keys())

    def get_columns(self, table, query, level=None):
        result = set()
        state = query.get_state(level)
        method = query.data('method')
        take = state.get('take', {})
        if "*" in take:
            value = take["*"]
            all_columns = set(self.all_columns(table, level))
            if value:
                result |= all_columns
            else:
                result -= all_columns

        remove = set()
        for k, v in take.items():
            if k == '*':
                continue
            if v:
                result.add(k)
            else:
                remove.add(k)

        if not result and method in ("get", "one"):
            # automatic * for get/one
            result = self.all_columns(table, level)

        for k in remove:
            if k in result:
                result.remove(k)

        return list(sorted(result))

    def get_data(self, table, query, count=False):
        field = query.data('field')
        if count:
            data = {'count': {'count': '*'}}
        elif field is not None:
            data = field
        else:
            data = query.data('take') or self.get_columns(table, query)
        return data

    def get_select(self, table, query, count=False, json=False):
        data = self.get_data(table, query, count=count)
        from_ = self.get_from(table, query)
        order = self.get_order(table, query)
        limit = self.get_limit(table, query)
        join = self.get_join(table, query)
        where = self.get_where(table, query)
        base = {
            'select': {
                'data': data,
                'join': join,
                'from': from_,
                'where': where,
                'order': order,
                'limit': limit
            }
        }
        if json:
            if count or not isinstance(data, (list, dict)):
                return base
            inner = []
            if isinstance(data, list):
                # fieldA -> fieldA
                for d in data:
                    inner.extend([f'`{d}`', d])
            else:
                for k, v in data.items():
                    inner.extend([f'`{k}`', v])

            # return the entire response as json
            return {
                'select': {
                    'data': {
                        'result': {
                            "json_aggregate": {
                                "json_build_object": inner
                            }
                        }
                    },
                    'from': {
                        'T': base
                    }
                }
            }
        return base

    def get_join(self, table, query):
        return query.data('join')

    def get_where(self, table, query):
        args = []
        pks = list(table.pks.keys())

        key = query.data('key')
        where = {}
        # add PK filters
        if key:
            if len(pks) > 1:
                assert(len(key) == len(pks))
                ands = []
                for i, pk in enumerate(pks):
                    ands.append({'=': [pk, literal(key[i])]})

                where = {'and': ands}
            else:
                pk = pks[0]
                where = {'=': [pk, literal(key)]}

        # add user filters
        wheres = query.data('where')
        if wheres:
            if where:
                # and together with PK
                where = {'and': [where, wheres]}
            else:
                where = wheres

        return where or None

    def get_from(self, table, query):
        return table.full_name

    def get_order(self, table, query):
        sort = query.data('sort')
        order = None
        if sort:
            order = []
            for column in sort:
                desc = False
                if column.startswith('-'):
                    desc = True
                    column = column[1:]
                order.append({
                    'by': column,
                    'desc': desc
                })
        return order

    def get_limit(self, table, query):
        if query.data('key'):
            return 1
        limit = query.data('limit')
        if limit:
            return int(limit)
        return None

    async def count(self, query, **kwargs):
        kwargs['count'] = True
        return await self.get(query, **kwargs)

    async def one(self, query, **kwargs):
        json = kwargs.get('json', False)
        result = await self.get(query, **kwargs)
        if json:
            if result[0] != '[' and result[1] != ']':
                raise ValueError('expecting result to be a JSON-encoded array')
            return result[1:-1]

        if isinstance(result, list):
            num = len(result)
            if num != 1:
                raise ValueError(f'expecting 1 record/value but got {num}')
            return result[0]
        else:
            # assume non-list results are already single Record or value
            return result

    async def get(self, query, **kwargs):
        """SELECT data in table

        Arguments:
            query: Query
            count: ?string
                if set, return count of rows instead of records
            connection: ?connection
            zql: if True, return the zql query instead of executing it
            json: if True, return entire response as a single JSON value

        Return:
            List of records: if no key is specified
            Record: if a key is specified
            Value: if counting, or if a key and field are both specified
        """
        field = query.data('field') is not None
        key = query.data('key') is not None
        connection = kwargs.get('connection', None)
        source = query.data('source')
        database = self.database
        table = await database.get_table(source, scope=self.scope)
        zql = kwargs.get('zql', False)
        count = kwargs.get('count', False)
        json = kwargs.get('json', False)
        select = self.get_select(table, query, count=count, json=json)

        if json or count or (field and key):
            method = 'query_one_value'
        elif key:
            method = 'query_one_row'
        elif field:
            method = 'query_one_column'
        else:
            method = 'query'

        if zql:
            # return the query instead of executing it
            return select

        return await getattr(self.database, method)(
            select,
            connection=connection
        )

    def quote(self, value):
        # add literal quoting, unless it is not a string
        # in which case return it directly
        # this allows us to pass in expressions as objects
        # or pass in normal strings
        return f"'{value}'" if isinstance(value, str) else value

    def get_set(self, values):
        return {
            key: self.quote(value) for key, value in values.items()
        }

    def get_returning(self, table, query):
        columns = self.get_columns(table, query)
        return columns if columns else None

    CHANGED_ROWS_REGEX = re.compile('[a-zA-Z]+ ([0-9]+)')
    ADDED_ROWS_REGEX = re.compile('[a-zA-Z]+ [0-9]+ ([0-9]+)')

    def get_added_rows(self, result, rows=None):
        regex = self.ADDED_ROWS_REGEX
        try:
            return self._get_changed_rows(regex, result, rows)
        except Exception:
            return rows

    def get_changed_rows(self, result, rows=None):
        regex = self.CHANGED_ROWS_REGEX
        return self._get_changed_rows(regex, result, rows)

    def _get_changed_rows(self, regex, result, rows):
        if isinstance(result, int):
            return result
        match = regex.match(result)
        return int(match.group(1)) if match else rows

    def get_values(self, values) -> tuple:
        if not values:
            return (None, None)

        result = []
        columns = []

        if isinstance(values, list):
            expected = set(values[0].keys())
            columns = list(sorted(expected))
            for value in values:
                cols = set(value.keys())

                if cols != expected:
                    raise ValueError(
                        f'expecting {expected} but got {cols}'
                    )

                subresult = []
                for column in columns:
                    if column not in value:
                        subresult.append({'default': None})
                    else:
                        subresult.append(self.quote(value[column]))
                result.append(subresult)

        elif isinstance(values, dict):
            # {"name": "test"}
            columns = list(sorted(values.keys()))
            for column in columns:
                result.append(self.quote(values[column]))

        return columns, result

    async def add(self, query, **kwargs):
        """INSERT data (or update on conflict)

        If there is a key in the query, add an
        ON CONFLICT (pk) DO UPDATE to simulate upsert

        Arguments:
            query: Query
            connection: ?connection
                useful for transactions

        Returns:
            numbers of records modified
        """
        # TODO: convert to zql
        values = query.data('values')
        connection = kwargs.get('connection')
        zql = kwargs.get('zql', False)
        # TODO: support ON CONFLICT
        # upsert = kwargs.get('upsert', True)

        # values is either a list or dict or None
        rows = 1
        if isinstance(values, list):
            # [{'name': 'kay'}, {'name': 'jay'}]
            rows = len(values)

        source = query.data('source')
        table = await self.database.get_table(source, scope=self.scope)
        returning = self.get_returning(table, query)
        columns, values = self.get_values(values)

        query = {
            'insert': {
                'table': table.full_name,
                'return': returning,
                'columns': columns,
                'values': values
            }
        }
        if zql:
            return query
        if returning:
            method = 'query' if rows > 1 else 'query_one_row'
        else:
            method = 'execute'
        result = await getattr(self.database, method)(
            query,
            connection=connection
        )
        if not returning:
            result = self.get_added_rows(result, rows=rows)
        return result

    async def set(self, query, **kwargs):
        """UPDATE data

        Arguments:
            query: Query
            count: ?string
            connection: ?connection
                useful for transactions

        Returns:
            number of records updated
        """
        # TODO: convert to zql
        field = query.data('field')
        values = query.data('values')
        connection = kwargs.get('connection')
        zql = kwargs.get('zql', False)
        assert(values)
        if not field:
            # values must be a dict with values
            # each key is a field name and each value is a field value
            assert(isinstance(values, dict))

        source = query.data('source')
        table = await self.database.get_table(source, scope=self.scope)
        returning = self.get_returning(table, query)
        where = self.get_where(table, query)
        if field is not None:
            values = {field: values}

        set_ = self.get_set(values)
        query = {
            'update': {
                'table': table.full_name,
                'set': set_,
                'where': where,
                'return': returning
            }
        }
        if zql:
            return query
        method = 'query' if returning else 'execute'
        result = await getattr(self.database, method)(
            query,
            connection=connection
        )
        if not returning:
            result = self.get_changed_rows(result)
        return result

    async def delete(self, query, **kwargs):
        """DELETE data

        Arguments:
            query: Query
            connection: ?connection
                useful for transactions

        Returns:
            number of records deleted
        """
        # TODO: convert to zql
        connection = kwargs.get('connection')
        source = query.data('source')
        table = await self.database.get_table(source, scope=self.scope)
        returning = self.get_returning(table, query)
        where = self.get_where(table, query)

        query = {
            'delete': {
                'table': table.full_name,
                'where': where,
                'return': returning
            }
        }
        method = 'query' if returning else 'execute'
        result = await getattr(self.database, method)(
            query,
            connection=connection
        )
        if not returning:
            result = self.get_changed_rows(result)
        return result

    async def truncate(self, query, **kwargs):
        """TRUNCATE data

        Arguments:
            query: Query
            connection: ?connection
                useful for transactions

        Returns:
            True
        """
        # TODO: convert to zql
        connection = kwargs.get('connection')
        source = query.data('source')
        table = await self.database.get_table(source, scope=self.scope)
        query = {'truncate': table.full_name}
        method = 'execute'
        return await getattr(self.database, method)(
            query,
            connection=connection
        )
