import re
import json
import ssl
import hashlib

from adbc.exceptions import NotIncluded
from adbc.utils import raise_not_implemented
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
from adbc.zql import parse_expression, parse_statement, build
from adbc.utils import aecho


EMPTY_CLAUSE = {'=': [1, 1]}
TAGGED_NUMBER_REGEX = re.compile(r'[a-zA-Z]+ ([0-9]+)')



def md5sum(t):
    t = str(t).encode('utf-8')
    return hashlib.md5(t).hexdigest()


def json_build_array(*args):
    return json.dumps(args)


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

    FUNCTIONS = {
        'group_concat'
    }
    default_schema = 'main'
    type = Backend.SQLITE
    dialect = Dialect(
        backend=type,
        style=ParameterStyle.QUESTION_MARK
    )

    @classmethod
    def build(cls, query: Union[dict, list]):
        return build(query, dialect=cls.dialect)

    @classmethod
    def parse_expression(cls, expression: str):
        """Return parsed zql expression"""
        return parse_expression(expression, cls.type)

    @classmethod
    def parse_statement(cls, statement: str):
        return parse_statement(statement, cls.type)

    @classmethod
    async def connect(cls, *args, **kwargs):
        if 'uri' not in kwargs:
            kwargs['uri'] = True
        if 'isolation_level' not in kwargs:
            # autocommit
            kwargs['isolation_level'] = None
        db = await connect(*args, **kwargs)
        await db.create_function('md5', 1, md5sum)
        await db.create_function('json_build_array', -1, json_build_array)
        db.row_factory = Row
        return db

    @classmethod
    async def copy_to_table(cls, connection, table_name, **kwargs):
        # sqlite copy-to-table: use insert
        return 'todo'

    @classmethod
    async def copy_from_table(cls, connection, table_name, **kwargs):
        # sqlite copy-from-table: use select
        return 'todo'

    @classmethod
    async def copy_from_query(cls, connection, query, params=None, **kwargs):
        # sqlite copy-from-query: use select
        return 'todo'

    @classmethod
    async def execute(cls, connection, query, params=None):
        params = params or []
        await connection.execute(query, params)
        # get changes
        async with connection.execute('select changes()') as cursor:
            row = await cursor.fetchone()
            return row[0]

    @classmethod
    async def cursor(cls, connection, query, params=None):
        params = params or []
        async with connection.execute(query, params) as cursor:
            async for row in cursor:
                yield row

    @classmethod
    async def fetch(cls, connection, query, params=None):
        params = params or []
        async with connection.execute(query, params) as cursor:
            return await cursor.fetchall()

    @classmethod
    def get_databases_query(cls, include, tag=None):
        return {'select': {'data': "'main'"}}

    @classmethod
    def get_namespaces_query(cls, include, tag=None):
        return {'select': {'data': "'main'"}}

    @classmethod
    def get_version_query():
        return {'select': {'data': {'version': {'sqlite_version': []}}}}

    @classmethod
    async def get_tables(cls, namespace, scope):
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
            statement = cls.parse_statement(sql)
            data = statement['create']['table']
            try:
                table = namespace.get_table(
                    name,
                    type='table',
                    columns=data.get('columns'),
                    constraints=data.get('constraints'),
                    indexes=data.get('indexes'),
                    scope=scope
                )
            except NotIncluded:
                pass
            else:
                tables.append(table)

        return tables

    @classmethod
    async def create_pool(cls, url, **kwargs):
        # ignore kwargs
        return SqlitePool(url)
