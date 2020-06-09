from urllib.parse import urlparse
from cached_property import cached_property

from .backends.postgres import PostgresBackend
from .database import Database
from .store import ParentStore, WithConfig


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
        self._backend = PostgresBackend()

    async def get_children(self):
        # ! databases are permanently cached after this query
        return await self.databases

    async def get_databases(self):
        return await self.main_database.query_one_row(
            *self._backend.get_query('databases')
        )

    def get_database(self, name, refresh=False):
        if name not in self._databases or refresh:
            config = self.get_child_config(name)
            if not config:
                raise Exception(f'{self}: database "{name}" is not included')

            self._databases[name] = Database(
                name,
                host=self,
                config=config
            )
        return self._databases[name]

    @cached_property
    def main_database(self):
        return self.get_database(self.dbname)

    @cached_property
    async def databases(self):
        databases = await self.get_databases()
        return {d.name: d for d in databases}
