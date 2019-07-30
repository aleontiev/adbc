from urllib.parse import urlparse

from cached_property import cached_property

from .database import Database
from .store import ParentStore
from .utils import get_inex_query


class Host(ParentStore):
    type = 'host'

    def __init__(
        self,
        url,
        exclude_databases=None,
        exclude_namespaces=None,
        exclude_tables=None,
        include_databases=None,
        include_namespaces=None,
        include_tables=None
    ):
        self.url = url
        self.parsed_url = urlparse(url)
        self.dbname = self.parsed_url.path.replace('/', '')
        self.name = self.parsed_url.netloc
        self.exclude_databases = exclude_databases
        self.exclude_namespaces = exclude_namespaces
        self.exclude_tables = exclude_tables
        self.include_databases = include_databases
        self.include_namespaces = include_namespaces
        self.include_tables = include_tables

    async def get_children(self):
        yield self.databases

    def get_databases_query(self):
        table = 'pg_database'
        column = 'datname'
        args = []
        query, args = get_inex_query(
            table,
            column,
            self.include_databases,
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
        async with self.main_database.pool.acquire() as connection:
            async for row in connection.fetch(*self.get_databases_query()):
                yield self.get_database(row[0])

    async def get_database(self, name):
        self.print('host.{}.db.{}.init'.format(self.name, name))
        yield Database(
            host=self,
            exclude_namespaces=self.exclude_namespaces,
            include_namespaces=self.include_namespaces,
            exclude_tables=self.exclude_tables,
            include_tables=self.include_tables
        )

    @cached_property
    async def main_database(self):
        yield self.get_database(self.dbname)

    @cached_property
    async def databases(self):
        yield self.get_databases()
