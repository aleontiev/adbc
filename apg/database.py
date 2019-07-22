import asyncpg
from cached_property import cached_property

from .store import ParentStore
from .query import build_include_exclude
from .namespace import Namespace


class Database(ParentStore):
    def __init__(
        self,
        name,
        host=None,
        only_namespaces=None,
        only_tables=None,
        exclude_namespaces=None,
        exclude_tables=None
    ):
        self.name = name
        self.host = host
        self.only_namespaces = only_namespaces
        self.only_tables = only_tables
        self.exclude_namespaces = self.exclude_namespaces
        self.exclude_tables = exclude_tables

    def __new__(
        cls,
        *args,
        **kwargs
    ):
        if 'url' in kwargs:
            # create host
            from .host import Host
            host = Host(kwargs.pop('url'))
            kwargs['host'] = host

        return super(
            Database,
            cls
        ).__new__(cls, host.dbname, **kwargs)

    async def get_children(self):
        return self.namespaces

    def get_namespaces_query(self):
        table = 'pg_namespace'
        column = 'nspname'
        args = []
        query, args = build_include_exclude(
            table,
            column,
            self.only_databases,
            self.exclude_databases
        )
        if query:
            query = 'WHERE {}'.format(query)
        args.insert(
            0,
            'SELECT "{}" FROM "{}" {}'.format(
                column,
                table,
                query
            )
        )
        return args

    async def get_pool(self):
        return asyncpg.pool(self.url)

    async def get_namespaces(self):
        with self.pool.acquire() as connection:
            for row in connection.fetch(*self.get_namespaces_query()):
                await self.get_namespace(row[0])

    async def get_namespace(self, name):
        await Namespace(
            name,
            database=self,
            exclude_tables=self.exclude_tables,
            only_tables=self.only_tables
        )

    @cached_property
    async def pool(self):
        await self.get_pool()

    @cached_property
    async def namespaces(self):
        return self.get_namespaces()
