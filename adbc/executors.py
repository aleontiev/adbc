import re
from .sql import list_columns, sort_columns, where_clause, should_escape


# TODO: support other backends
def get_executor(table):
    return PostgresExecutor(table)


class PostgresExecutor(object):
    def __init__(self, table):
        self.table = table
        self.database = table.database

    def get_delete(self, query):
        table = self.table
        schema = table.namespace
        return f'DELETE FROM "{schema.name}"."{table.name}"'

    def get_insert(self, query):
        table = self.table
        schema = table.namespace
        body = query.data('body')
        columns = list_columns(
            body.keys() if isinstance(body, dict) else body[0].keys()
        )
        return f'INSERT INTO "{schema.name}"."{table.name}" ({columns})'

    def get_select(self, query, count=None):
        field = query.data('field')
        if field is not None:
            columns = list_columns([field])
        else:
            columns = list_columns(query.columns())
        if count:
            columns = count
        return f"SELECT {columns}"

    def get_where(self, query):
        args = []
        pks = self.table.pks

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

    def get_from(self, query):
        table = self.table
        schema = table.namespace
        return f'FROM "{schema.name}"."{table.name}"'

    def get_update(self, query):
        table = self.table
        schema = table.namespace
        return f'UPDATE "{schema.name}"."{table.name}"'

    def get_order(self, query):
        sort = query.data('sort')
        if sort:
            columns = sort_columns(sort)
            return f'ORDER BY {columns}'
        return ''

    def get_limit(self, query):
        if query.data('key'):
            return 'LIMIT 1'
        limit = query.data('limit')
        if limit:
            return f'LIMIT {int(limit)}'
        return ''

    async def count(self, query, **kwargs):
        kwargs['count'] = 'count(*)'
        return await self.get(query, **kwargs)

    async def get(self, query, **kwargs):
        """SELECT data in table

        Arguments:
            query: Query
            count: ?string
                if set, return count of rows instead of records
            connection: ?asyncpg.connection

        Return:
            List of records: if no key is specified
            Record: if a key is specified
            Value: if counting, or if a key and field are both specified
        """
        field = query.data('field') is not None
        key = query.data('key') is not None
        connection = kwargs.get('connection', None)

        count = kwargs.get('count', False)
        select = self.get_select(query, count=count)
        where = self.get_where(query)
        if where:
            where, *args = where
        else:
            where = None
            args = []
        from_ = self.get_from(query)
        order = self.get_order(query)
        limit = self.get_limit(query)

        sql = self.build_sql(
            select,
            from_,
            where,
            order,
            limit,
            args
        )
        method = (
            'query_one_value' if count or (field and key)
            else 'query_one_row' if key else 'query'
        )
        return await getattr(self.database, method)(
            *sql,
            connection=connection
        )

    def get_set(self, body, args):
        multiple = len(body) > 1
        params = []
        columns = []
        for key, value in body.items():
            args.append(value)
            columns.append(key)
            params.append(f'${len(args)}')

        columns = list_columns(columns)
        params = ', '.join(params)
        if multiple:
            columns = f'({columns})'
            params = f'({params})'
        return f'SET {columns} = {params}'

    def get_returning(self, query):
        columns = query.columns()
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

    def get_values(self, body, args):
        values = []
        expected_columns = list(body[0].keys())
        for data in body:
            value = []
            columns = []
            for k, v in data.items():
                columns.append(k)
                if should_escape(v):
                    args.append(v)
                    value.append(f'${len(args)}')
                else:
                    value.append(v)
            assert(columns == expected_columns)
            values.append(f"({', '.join(value)})")
        values = ', \n'.join(values)
        return f"VALUES {values}"

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
        body = query.data('body')
        connection = kwargs.get('connection')
        # TODO: support ON CONFLICT
        # upsert = kwargs.get('upsert', True)

        # body is either a list of dicts or a dict
        assert(body)
        if isinstance(body, dict):
            body = [body]

        returning = self.get_returning(query)
        args = []
        insert = self.get_insert(query)
        values = self.get_values(body, args)

        sql = self.build_sql(
            insert,
            values,
            returning,
            args
        )
        method = 'query' if returning else 'execute'
        result = await getattr(self.database, method)(
            *sql,
            connection=connection
        )
        if not returning:
            result = self.get_inserted_rows(result)
        return result

    MODIFIED_ROWS_REGEX = re.compile('[A-Z]+ ([0-9])+')

    def get_deleted_rows(self, result):
        return self.get_updated_rows(result)

    def get_updated_rows(self, result):
        match = self.MODIFIED_ROWS_REGEX.match(result)
        return int(match.group(1))

    INSERTED_ROWS_REGEX = re.compile('INSERT [0-9]+ ([0-9]+)')

    def get_inserted_rows(self, result):
        match = self.INSERTED_ROWS_REGEX.match(result)
        return int(match.group(1))

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
        body = query.data('body')
        connection = kwargs.get('connection')
        assert(body)
        if not field:
            # body must be a dict with values
            # each key is a field name and each value is a field value
            assert(isinstance(body, dict))

        returning = self.get_returning(query)

        where = self.get_where(query)
        if where:
            where, *args = where
        else:
            where = None
            args = []

        if field is not None:
            body = {field: body}

        set_ = self.get_set(body, args)

        update = self.get_update(query)
        sql = self.build_sql(
            update,
            set_,
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
            result = self.get_updated_rows(result)
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
        returning = self.get_returning(query)

        where = self.get_where(query)
        if where:
            where, *args = where
        else:
            where = None
            args = []

        delete = self.get_delete(query)
        sql = self.build_sql(
            delete,
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
            result = self.get_deleted_rows(result)
        return result
