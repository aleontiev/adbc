from urllib.parse import urlparse
from cached_property import cached_property

from adbc.backends.postgres import PostgresBackend
from adbc.logging import Loggable
from adbc.scope import WithScope
from adbc.cache import WithCache

from .database import Database


class Host(Loggable, WithScope):
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
        self._backend = PostgresBackend()

    async def get_children(self, scope=None):
        scope = scope or self.scope
        return await self.cache_by_async(
            'children',
            scope,
            lambda: self.get_databases(scope=scope),
        )

    async def get_databases(self, scope=None):
        databases = await self.database.query_one_column(
            *self._backend.get_query('databases')
        )
        return [
            self.get_database(name, scope=scope) for name in databases
        ]

    def get_database(self, name, scope=None):
        scope = scope or self.scope
        return self.cache_by(
            'databases',
            {'scope': scope, 'name': name},
            lambda: self._get_database(name, scope=scope)
        )

    def _get_database(self, name, scope):
        scope = self.get_child_scope(name, scope=scope)
        if not scope:
            raise Exception(
                f'{self}: database "{name}" is not included'
            )

        return Database(
            name,
            host=self,
            scope=scope
        )

    @cached_property
    def database(self):
        return self.get_database(self.dbname)
