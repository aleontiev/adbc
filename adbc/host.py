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
        self._databases = {}

    async def get_children(self):
        # ! databases are permanently cached after this query
        return await self.databases

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

    async def get_database(self, name, refresh=False):
        if name not in self._databases or refresh:
            config = self.get_child_config(name)
            if not config:
                raise Exception(f'{self}: database "{name}" is not included')

            self._databases[name] = Database(
                name,
                host=self,
                config=config
            )
        yield self._databases[name]

    @cached_property
    async def main_database(self):
        yield self.get_database(self.dbname)

    @cached_property
    async def databases(self):
        databases = await self.get_databases()
        return {d.name for d in databases}
