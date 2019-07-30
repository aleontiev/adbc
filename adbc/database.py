import asyncpg
import asyncio
from cached_property import cached_property

from jsondiff import diff
from .store import ParentStore
from .utils import get_inex_query
from .namespace import Namespace


class Database(ParentStore):
    type = 'db'

    def __init__(
        self,
        name=None,
        host=None,
        url=None,
        include_namespaces=None,
        include_tables=None,
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
        self.parent = self.host = host
        self.include_namespaces = include_namespaces
        self.include_tables = include_tables
        self.exclude_namespaces = exclude_namespaces
        self.exclude_tables = exclude_tables
        self.verbose = verbose
        self.tag = tag

    def get_namespaces_query(self):
        table = "pg_namespace"
        column = "nspname"
        query, args = get_inex_query(
            table, column, self.include_namespaces, self.exclude_namespaces
        )
        if query:
            query = "WHERE {}".format(query)
        args.insert(0, 'SELECT "{}" FROM "{}" {}'.format(column, table, query))
        return args

    def get_namespace(self, name):
        self.print('db.{}.ns.{}.init'.format(self.name, name))
        return Namespace(
            name,
            database=self,
            exclude_tables=self.exclude_tables,
            include_tables=self.include_tables,
            verbose=self.verbose,
            tag=self.tag
        )

    async def diff(self, other):
        data = self.get_diff_data()
        other_data = other.get_diff_data()
        data, other_data = await asyncio.gather(data, other_data)
        return diff(data, other_data, syntax='symmetric')

    async def get_pool(self):
        return await asyncpg.create_pool(dsn=self.host.url, max_size=20)

    async def get_children(self):
        query = self.get_namespaces_query()
        pool = await self.pool
        async with pool.acquire() as connection:
            async with connection.transaction():
                async for row in connection.cursor(*query):
                    yield self.get_namespace(row[0])

    @cached_property
    async def pool(self):
        return await self.get_pool()
