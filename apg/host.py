from urllib.parse import urlparse

from cached_property import cached_property

from .database import Database
from .store import ParentStore
from .query import build_include_exclude


class Host(ParentStore):
    def __init__(
        self,
        url,
        exclude_databases=None,
        exclude_namespaces=None,
        exclude_tables=None,
        only_databases=None,
        only_namespaces=None,
        only_tables=None
    ):
        self.url = url
        self.dbname = urlparse(url).path.replace('/', '')
        self.exclude_databases = exclude_databases
        self.exclude_namespaces = exclude_namespaces
        self.exclude_tables = exclude_tables
        self.only_databases = only_databases
        self.only_namespaces = only_namespaces
        self.only_tables = only_tables

    async def get_children(self):
        return self.databases

    def get_databases_query(self):
        table = 'pg_database'
        column = 'datname'
        args = []
        query, args = build_include_exclude(
            table,
            column,
            self.only_databases,
            self.exclude_databases
        )
        if query:
            query = ' AND {}'.format(query)
        args.insert(
            0,
            'SELECT "{}" FROM "{}" WHERE datistemplate = false {}'.format(
                column,
                table,
                query
            )
        )
        return args

    async def get_databases(self):
        with self.main_database.pool.acquire() as connection:
            for row in connection.fetch(*self.get_databases_query()):
                await self.get_database(row[0])

    async def get_database(self, name):
        await Database(
            host=self,
            exclude_namespaces=self.exclude_namespaces,
            only_namespaces=self.only_namespaces,
            exclude_tables=self.exclude_tables,
            only_tables=self.only_tables
        )

    @cached_property
    async def main_database(self):
        await self.get_database(self.dbname)

    @cached_property
    async def databases(self):
        return self.get_databases()
