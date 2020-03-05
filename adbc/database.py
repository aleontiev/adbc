import asyncpg
import asyncio
import json
from cached_property import cached_property

from jsondiff import diff
from .store import ParentStore, WithInclude
from .utils import get_include_query, get_server_version
from .namespace import Namespace


DATABASE_VERSION_QUERY = 'SELECT version()'

DIFF_NO_SYMBOLS = True


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

    async def stream(self, *query):
        pool = await self.pool
        async with pool.acquire() as connection:
            async with connection.transaction():
                async for row in connection.cursor(*query):
                    yield row

    async def query(self, *query, many=True, columns=True):
        pool = await self.pool
        async with pool.acquire() as connection:
            async with connection.transaction():
                results = await connection.fetch(*query)
                if many:
                    return results if columns else [r[0] for r in results]
                else:
                    result = results[0]
                    return result if columns else result[0]

    async def query_one_row(self, *query, as_=None):
        result = await self.query(*query, many=False, columns=True)
        if as_:
            return as_(result)

    async def query_one_value(self, *query):
        return await self.query(*query, many=False, columns=False)

    async def get_version(self):
        version = await self.query_one_value(DATABASE_VERSION_QUERY)
        return get_server_version(version)

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
        if DIFF_NO_SYMBOLS:
            return json.loads(diff(data, other_data, syntax='symmetric', dump=True))
        else:
            return diff(data, other_data, syntax='symmetric')

    async def get_pool(self):
        return await asyncpg.create_pool(dsn=self.host.url, max_size=20)

    async def get_children(self):
        query = self.get_namespaces_query()
        async for row in self.stream(*query):
            yield self.get_namespace(row[0])

    @cached_property
    async def pool(self):
        return await self.get_pool()
