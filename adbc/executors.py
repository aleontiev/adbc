import re

from adbc.sql import (
    list_columns,
    sort_columns,
    where_clause,
    should_escape,
    get_tagged_number
)


# TODO: support other backends
def get_executor(source):
    return PostgresExecutor(source)


class PostgresExecutor(object):
    def __init__(self, database):
        self.database = database

    def get_insert(self, table, query):
        values = query.data('values')
        columns = list_columns(
            sorted(values.keys() if isinstance(values, dict) else values[0].keys())
        )
        return f'INSERT INTO {table.sql_name} ({columns})'

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

    def get_select(self, table, query, count=None):
        field = query.data('field')
        if count:
            columns = 'count(*)'
        elif field is not None:
            columns = list_columns([field])
        else:
            columns = list_columns(self.get_columns(table, query))
        return f"SELECT {columns}"

    def get_where(self, table, query):
        args = []
        pks = table.pks

        key = query.data('key')
        where = {}
        # add PK filters
        if key:
            if len(pks) > 1:
                assert(len(key) == len(pks))
                ands = []
                for i, pk in enumerate(pks):
                    ands.append({pk: key[i]})

                where = {'.and': ands}
            else:
                pk = pks[0]
                where = {pk: key}

        # add user filters
        wheres = query.data('where')
        if wheres:
            if where:
                # and together with PK
                where = {'.and': [where, wheres]}
            else:
                where = wheres

        where = where_clause(where, args)
        return [f"WHERE {where}", *args] if where else []

    def get_from(self, table, query):
        return f'FROM {table.sql_name}'

    def get_joins(self, table, query):
        # TODO: implement joins
        return ''

    def get_update(self, table, query):
        return f'UPDATE {table.sql_name}'

    def get_order(self, table, query):
        sort = query.data('sort')
        if sort:
            columns = sort_columns(sort)
            return f'ORDER BY {columns}'
        return ''

    def get_limit(self, table, query):
        if query.data('key'):
            return 'LIMIT 1'
        limit = query.data('limit')
        if limit:
            return f'LIMIT {int(limit)}'
        return ''

    async def count(self, query, **kwargs):
        kwargs['count'] = True
        return await self.get(query, **kwargs)

    async def one(self, query, **kwargs):
        result = await self.get(query, **kwargs)
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
            connection: ?asyncpg.connection
            sql: if True, return the query instead

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
        table = await database.get_table(source)
        sql = kwargs.get('sql', None)
        count = kwargs.get('count', False)
        select = self.get_select(table, query, count=count)
        where = self.get_where(table, query)
        if where:
            where, *args = where
        else:
            where = None
            args = []
        from_ = self.get_from(table, query)
        joins = self.get_joins(table, query)
        order = self.get_order(table, query)
        limit = self.get_limit(table, query)

        query = self.build_sql(
            select,
            from_,
            joins,
            where,
            order,
            limit,
            args
        )
        if sql:
            # just return the query
            return query
        if count or (field and key):
            method = 'query_one_value'
        elif key:
            method = 'query_one_row'
        elif field:
            method = 'query_one_column'
        else:
            method = 'query'
        return await getattr(self.database, method)(
            *query,
            connection=connection
        )

    def get_set(self, values, args):
        multiple = len(values) > 1
        params = []
        columns = []
        for key, value in values.items():
            args.append(value)
            columns.append(key)
            params.append(f'${len(args)}')

        columns = list_columns(columns)
        params = ', '.join(params)
        if multiple:
            columns = f'({columns})'
            params = f'({params})'
        return f'SET {columns} = {params}'

    def get_returning(self, table, query):
        columns = self.get_columns(table, query)
        if columns:
            # if no columns are passed explicitly,
            # do not use returning
            return f'RETURNING {list_columns(columns)}'
        else:
            return ''

    def build_sql(self, *args):
        last = args[-1]
        args = args[:-1]
        sql = '\n'.join([a for a in args if a])
        return (sql, *last)

    INSERTED_ROWS_REGEX = re.compile('INSERT [0-9]+ ([0-9]+)')

    def get_inserted_rows(self, result):
        match = self.INSERTED_ROWS_REGEX.match(result)
        return int(match.group(1))

    def get_values(self, values, args):
        output = []
        expected_columns = list(sorted(values[0].keys()))
        for data in values:
            value = []
            columns = []
            for k, v in sorted(data.items(), key=lambda x: x[0]):
                columns.append(k)
                if should_escape(v):
                    args.append(v)
                    value.append(f'${len(args)}')
                else:
                    value.append(v)
            if columns != expected_columns:
                raise ValueError(
                    f'expecting {expected_columns} but got {columns}'
                )
            output.append(f"({', '.join(value)})")
        output = ', \n'.join(output)
        return f"VALUES {output}"

    async def add(self, query, **kwargs):
        """INSERT data (or update on conflict)

        If there is a key in the query, add an
        ON CONFLICT (pk) DO UPDATE to simulate upsert

        Arguments:
            query: Query
            upsert: bool
                default: True
            connection: *asyncpg.connection
                useful for transactions

        Returns:
            numbers of records modified
        """
        values = query.data('values')
        connection = kwargs.get('connection')
        sql = kwargs.get('sql', False)
        # TODO: support ON CONFLICT
        # upsert = kwargs.get('upsert', True)

        # values is either a list of dicts or a dict
        assert(values)
        multiple = True
        if isinstance(values, dict):
            multiple = False
            values = [values]

        source = query.data('source')
        table = await self.database.get_table(source)
        returning = self.get_returning(table, query)
        args = []
        insert = self.get_insert(table, query)
        values = self.get_values(values, args)

        query = self.build_sql(
            insert,
            values,
            returning,
            args
        )
        if sql:
            return query
        if returning:
            method = 'query' if multiple else 'query_one_row'
        else:
            method = 'execute'
        result = await getattr(self.database, method)(
            *query,
            connection=connection
        )
        if not returning:
            result = self.get_inserted_rows(result)
        return result

    async def set(self, query, **kwargs):
        """UPDATE data

        Arguments:
            query: Query
            count: ?string
            connection: ?asyncpg.connection
                useful for transactions

        Returns:
            number of records updated
        """
        field = query.data('field')
        values = query.data('values')
        connection = kwargs.get('connection')
        sql = kwargs.get('sql', False)
        assert(values)
        if not field:
            # values must be a dict with values
            # each key is a field name and each value is a field value
            assert(isinstance(values, dict))

        source = query.data('source')
        table = await self.database.get_table(source)
        returning = self.get_returning(table, query)
        where = self.get_where(table, query)
        if where:
            where, *args = where
        else:
            where = None
            args = []

        if field is not None:
            values = {field: values}

        set_ = self.get_set(values, args)

        update = self.get_update(table, query)
        query = self.build_sql(
            update,
            set_,
            where,
            returning,
            args
        )
        if sql:
            return query
        method = 'query' if returning else 'execute'
        result = await getattr(self.database, method)(
            *query,
            connection=connection
        )
        if not returning:
            result = get_tagged_number(result)
        return result

    async def delete(self, query, **kwargs):
        """DELETE data

        Arguments:
            query: Query
            connection: ?asyncpg.connection
                useful for transactions

        Returns:
            number of records deleted
        """
        connection = kwargs.get('connection')
        source = query.data('source')
        table = await self.database.get_table(source)
        returning = self.get_returning(table, query)
        where = self.get_where(table, query)
        if where:
            where, *args = where
        else:
            where = None
            args = []

        from_ = self.get_from(table, query)
        sql = self.build_sql(
            'DELETE',
            from_,
            where,
            returning,
            args
        )
        method = 'query' if returning else 'execute'
        result = await getattr(self.database, method)(
            *sql,
            connection=connection
        )
        if not returning:
            result = get_tagged_number(result)
        return result

    async def truncate(self, query, **kwargs):
        """TRUNCATE data

        Arguments:
            query: Query
            connection: ?asyncpg.connection
                useful for transactions

        Returns:
            True
        """
        connection = kwargs.get('connection')
        source = query.data('source')
        table = await self.database.get_table(source)
        sql = f'TRUNCATE {table.sql_name}'
        method = 'execute'
        return await getattr(self.database, method)(
            sql,
            connection=connection
        )
