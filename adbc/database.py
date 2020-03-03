import asyncpg
import asyncio
from cached_property import cached_property

from jsondiff import diff
from .store import ParentStore, WithInclude
from .utils import get_include_query, get_server_version
from .namespace import Namespace


class Database(WithInclude, ParentStore):
    type = 'db'

    def __init__(
        self,
        name=None,
        host=None,
        url=None,
        include=None,
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
        self.include = include
        self.verbose = verbose
        self.tag = tag

    @cached_property
    async def version(self):
        return await self.get_version()

    async def get_version(self):
        query = ['SELECT version()']
        pool = await self.pool
        async with pool.acquire() as connection:
            async with connection.transaction():
                async for row in connection.cursor(*query):
                    return get_server_version(row[0])

    def get_namespaces_query(self):
        table = "pg_namespace"
        column = "nspname"
        query, args = get_include_query(
            self.include, table, column
        )
        if query:
            query = "WHERE {}".format(query)
        args.insert(0, 'SELECT "{}" FROM "{}" {}'.format(column, table, query))
        return args

    def get_namespace(self, name):
        include = self.get_include(name)
        if not include:
            raise Exception(f'{self}: namespace "{name}" is not included')

        self.log(
            'db.{}.ns.{}.init({})'.format(self.name, name, include)
        )
        return Namespace(
            name,
            database=self,
            include=include,
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
