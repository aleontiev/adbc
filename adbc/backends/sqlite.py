import re
import json
import ssl

from sqliteschema import extractor
from aiosqlite import connect
from typing import Union
from .base import DatabaseBackend
from cached_property import cached_property
from asyncpg import create_pool, connect
from urllib.parse import urlparse, parse_qs, urlencode
from adbc.preql.dialect import Dialect, Backend, ParameterStyle
from adbc.preql import parse, build


EMPTY_CLAUSE = {'=': [1, 1]}
TAGGED_NUMBER_REGEX = re.compile(r'[a-zA-Z]+ ([0-9]+)')


class SqlitePool():
    def __init__(self, url, **kwargs):
        self.url = url
        self.kwargs = kwargs

    async def acquire(self):
        url = self.url
        if not url:
            raise ValueError('acquire: url is required')
        return await connect(url)


class SqliteBackend(DatabaseBackend):
    """Sqlite backend based on aiosqlite"""

    default_schema = 'main'
    dialect = Dialect(
        backend=Backend.SQLITE,
        style=ParameterStyle.QUESTION
    )

    def build(self, query: Union[dict, list]):
        return build(query, dialect=self.dialect)

    def parse_expression(self, expression: str):
        """Return parsed PreQL expression"""
        return parse(expression, Backend.SQLITE)

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
        cursor = await connection.execute(query, params)
        return cursor.fetchall()

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
    async def create_pool(url, **kwargs):
        return await SqlitePool(url, **kwargs)

    @staticmethod
    def get_tables(namespace, scope):
        # use sqliteschema package
        extractor = sqliteschema.SQLiteSchemaExtractor(namespace.database.host.path)
        schema = extractor.fetch_database_schema_as_dict()
        return schema

