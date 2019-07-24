import asyncpg
import asyncio
from cached_property import cached_property

from deepdiff import DeepDiff
from .store import ParentStore
from .query import build_include_exclude
from .namespace import Namespace


class Database(ParentStore):
    def __init__(
        self,
        name=None,
        host=None,
        url=None,
        only_namespaces=None,
        only_tables=None,
        exclude_namespaces=None,
        exclude_tables=None,
        tag=None,
        verbose=False,
    ):
        if url and not host:
            from .host import Host

            host = Host(url)
            if not name:
                name = host.dbname

        self.name = name
        self.host = host
        self.only_namespaces = only_namespaces
        self.only_tables = only_tables
        self.exclude_namespaces = exclude_namespaces
        self.exclude_tables = exclude_tables
        self.verbose = verbose
        self.tag = tag

    async def diff(self, other):
        data = self.get_diff_data()
        other_data = other.get_diff_data()
        data, other_data = await asyncio.gather(data, other_data)
        return DeepDiff(data, other_data)

    async def get_children(self):
        namespaces = await self.namespaces
        return namespaces

    def get_namespaces_query(self):
        table = "pg_namespace"
        column = "nspname"
        query, args = build_include_exclude(
            table, column, self.only_namespaces, self.exclude_namespaces
        )
        if query:
            query = "WHERE {}".format(query)
        args.insert(0, 'SELECT "{}" FROM "{}" {}'.format(column, table, query))
        return args

    async def get_pool(self):
        pool = await asyncpg.create_pool(dsn=self.host.url, max_size=20)
        return pool

    async def get_namespaces(self):
        query = self.get_namespaces_query()
        pool = await self.pool
        namespaces = []
        async with pool.acquire() as connection:
            for row in await connection.fetch(*query):
                namespace = self.get_namespace(row[0])
                namespaces.append(namespace)
            self.print(
                "<- {}.database.{}.namespaces = {}".format(
                    self.tag or '',
                    self.name, len(namespaces))
            )
        return namespaces

    def get_namespace(self, name):
        return Namespace(
            name,
            database=self,
            exclude_tables=self.exclude_tables,
            only_tables=self.only_tables,
            verbose=self.verbose,
            tag=self.tag
        )

    @cached_property
    async def pool(self):
        return await self.get_pool()

    @cached_property
    async def namespaces(self):
        return await self.get_namespaces()
