import re
import json
import ssl

from adbc.utils import raise_not_implemented, parse_create_table
try:
    from aiosqlite import connect, Row
except ImportError:
    # this backend will fail
    # but allows import to succeed
    connect = raise_not_implemented('install aiosqlite')
    Row = raise_not_implemented('install aiosqlite')

from typing import Union
from .base import DatabaseBackend
from cached_property import cached_property
from urllib.parse import urlparse, parse_qs, urlencode
from adbc.zql.dialect import Dialect, Backend, ParameterStyle
from adbc.zql import parse, build
from adbc.utils import aecho


EMPTY_CLAUSE = {'=': [1, 1]}
TAGGED_NUMBER_REGEX = re.compile(r'[a-zA-Z]+ ([0-9]+)')



class SqlitePoolContext(object):
    def __init__(self, url):
        self.url = url
        self.connection = None
        self.closed = False

    async def __aenter__(self):
        self.connection = await SqliteBackend.connect(self.url)
        return self.connection

    async def __aexit__(self, *args):
        await self.close()

    async def close(self):
        if self.connection and not self.closed:
            await self.connection.close()
            self.connection = None
            self.closed = True


class SqlitePool:
    # naive pool
    def __init__(self, url):
        self.url = url
        self.acquired = []

    def acquire(self):
        context = SqlitePoolContext(self.url)
        self.acquired.append(context)
        return context

    async def close(self):
        # close any contexts
        for acquired in self.acquired:
            await acquired.close()
        self.acquired = []



class SqliteBackend(DatabaseBackend):
    """Sqlite backend based on aiosqlite"""

    default_schema = 'main'
    dialect = Dialect(
        backend=Backend.SQLITE,
        style=ParameterStyle.QUESTION_MARK
    )

    def build(self, query: Union[dict, list]):
        return build(query, dialect=self.dialect)

    def parse_expression(self, expression: str):
        """Return parsed zql expression"""
        return parse(expression, Backend.SQLITE)

    @staticmethod
    async def connect(*args, **kwargs):
        if 'uri' not in kwargs:
            kwargs['uri'] = True
        db = await connect(*args, **kwargs)
        db.row_factory = Row
        return db

    async def copy_to_table(self, connection, table_name, **kwargs):
        # sqlite copy-to-table: use insert
        return 'todo'

    async def copy_from_table(self, connection, table_name, **kwargs):
        # sqlite copy-from-table: use select
        return 'todo'

    async def copy_from_query(self, connection, query, params=None, **kwargs):
        # sqlite copy-from-query: use select
        return 'todo'

    async def execute(self, connection, query, params=None):
        params = params or []
        return await connection.execute(query, params)

    async def cursor(self, connection, query, params=None):
        params = params or []
        async with connection.execute(query, params) as cursor:
            async for row in cursor:
                yield row

    async def fetch(self, connection, query, params=None):
        params = params or []
        async with connection.execute(query, params) as cursor:
            return await cursor.fetchall()

    @staticmethod
    def get_databases_query(include, tag=None):
        return {'select': {'data': "'main'"}}

    @staticmethod
    def get_namespaces_query(include, tag=None):
        return {'select': {'data': "'main'"}}

    @staticmethod
    def get_version_query():
        return {'select': {'data': {'version': {'sqlite_version': []}}}}

    @staticmethod
    async def get_tables(namespace, scope):
        # use DdlParse package + sqlite_master table
        tables = []
        query = {
            'select': {
                'data': ['tbl_name', 'sql'],
                'from': 'sqlite_master',
                'where': {'=': ['type', '`table`']}
            }
        }
        database = namespace.database
        for row in await database.query(query):
            name = row['tbl_name']
            sql = row['sql']
            columns, constraints, indexes = parse_create_table(sql)
            try:
                table = namespace.get_table(
                    name,
                    type='table',
                    columns=columns,
                    constraints=constraints,
                    indexes=indexes,
                    scope=scope
                )
            except NotIncluded:
                pass
            else:
                tables.append(table)

        return tables

    @staticmethod
    async def create_pool(url, **kwargs):
        # ignore kwargs
        return SqlitePool(url)
