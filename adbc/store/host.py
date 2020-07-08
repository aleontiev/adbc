from urllib.parse import urlparse
from cached_property import cached_property

from adbc.backends.postgres import PostgresBackend
from adbc.logging import Loggable
from adbc.scope import WithScope

from .database import Database


class Host(Loggable, WithScope):
    type = 'host'
    child_key = 'databases'

    def __init__(
        self,
        url,
        scope=None
    ):
        self.url = url
        self.parsed_url = urlparse(url)
        self.dbname = self.parsed_url.path.replace('/', '')
        self.name = self.parsed_url.netloc
        self.scope = scope
        self._databases = {}
        self._backend = PostgresBackend()

    async def get_children(self):
        # ! databases are permanently cached after this query
        return await self.get_databases()

    async def get_databases(self):
        return await self.database.query_one_row(
            *self._backend.get_query('databases')
        )

    def get_database(self, name, scope=None, refresh=False):
        if name not in self._databases or refresh or scope is not None:
            scope = self.get_child_scope(name, scope=scope)
            if not scope:
                raise Exception(
                    f'{self}: database "{name}" is not included'
                )

            self._databases[name] = Database(
                name,
                host=self,
                scope=scope
            )
        return self._databases[name]

    @cached_property
    def database(self):
        return self.get_database(self.dbname)
