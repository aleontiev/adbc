from urllib.parse import urlparse
from cached_property import cached_property

from .database import Database
from .store import ParentStore, WithConfig
from .utils import get_include_query


class Host(WithConfig, ParentStore):
    type = 'host'
    child_key = 'databases'

    def __init__(
        self,
        url,
        config=None
    ):
        self.url = url
        self.parsed_url = urlparse(url)
        self.dbname = self.parsed_url.path.replace('/', '')
        self.name = self.parsed_url.netloc
        self.config = config

    async def get_children(self):
        yield self.databases

    def get_databases_query(self):
        table = 'pg_database'
        column = 'datname'
        args = []
        include = self.get_child_include()
        query, args = get_include_query(
            include,
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
        return await self.main_database.query_one_row(*self.get_databases_query())

    async def get_database(self, name):
        config = self.get_child_config(name)
        if not config:
            raise Exception(f'{self}: database "{name}" is not included')

        yield Database(
            host=self,
            config=config
        )

    @cached_property
    async def main_database(self):
        yield self.get_database(self.dbname)

    @cached_property
    async def databases(self):
        yield self.get_databases()
