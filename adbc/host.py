from urllib.parse import urlparse
from cached_property import cached_property

from .database import Database
from .store import ParentStore, WithInclude
from .utils import get_include_query


class Host(WithInclude, ParentStore):
    type = 'host'

    def __init__(
        self,
        url,
        include=True
    ):
        self.url = url
        self.parsed_url = urlparse(url)
        self.dbname = self.parsed_url.path.replace('/', '')
        self.name = self.parsed_url.netloc
        self.include = include

    async def get_children(self):
        yield self.databases

    def get_databases_query(self):
        table = 'pg_database'
        column = 'datname'
        args = []
        query, args = get_include_query(
            self.include,
            table,
            column,
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
        self.log('host.{}.db.{}.init'.format(self.name, name))
        include = self.get_include(name)
        if not include:
            raise Exception(f'{self}: database {name} is not included')

        yield Database(
            host=self,
            include=include
        )

    @cached_property
    async def main_database(self):
        yield self.get_database(self.dbname)

    @cached_property
    async def databases(self):
        yield self.get_databases()
