import asyncpg
import asyncio
from cached_property import cached_property

from jsondiff import diff

from .exceptions import NotIncluded
from .store import ParentStore, WithConfig
from .utils import get_include_query, get_server_version
from .namespace import Namespace

DATABASE_VERSION_QUERY = 'SELECT version()'


class Database(WithConfig, ParentStore):
    child_key = 'schemas'
    type = 'db'

    def __init__(
        self,
        name=None,
        host=None,
        url=None,
        config=None,
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
        self.config = config
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

    async def execute(self, *query):
        pool = await self.pool
        async with pool.acquire() as connection:
            async with connection.transaction():
                return await connection.execute(*query)

    async def query(self, *query, many=True, columns=True):
        pool = await self.pool
        async with pool.acquire() as connection:
            async with connection.transaction():
                try:
                    results = await connection.fetch(*query)
                except Exception as e:
                    if len(query) == 1:
                        query = query[0]
                    raise Exception(
                        f'query failed:\n======\n{query}\n======\n'
                        f'{e.__class__}: {e}'
                    )
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
        include = self.get_child_include()
        query, args = get_include_query(
            include, table, column
        )
        if query:
            query = "WHERE {}".format(query)
        args.insert(0, 'SELECT "{}"\nFROM "{}" {}'.format(column, table, query))
        return args

    def get_namespace(self, name):
        config = self.get_child_config(name)
        return Namespace(
            name,
            database=self,
            config=config,
            verbose=self.verbose,
            tag=self.tag
        )

    async def diff(self, other, translate=None):
        data = self.get_diff_data()
        other_data = other.get_diff_data()
        data, other_data = await asyncio.gather(data, other_data)

        if translate:
            # translate after both diffs have already been captured
            for key, value in translate.items():
                if key == value:
                    continue

                # source schema "key" is the same as target schema "value"
                if key in data:
                    data[value] = data[key]
                    data.pop(key)

        return diff(data, other_data, syntax='symmetric')

    async def get_pool(self):
        return await asyncpg.create_pool(dsn=self.host.url, max_size=20)

    async def get_children(self):
        query = self.get_namespaces_query()
        async for row in self.stream(*query):
            try:
                yield self.get_namespace(row[0])
            except NotIncluded:
                pass

    @cached_property
    async def pool(self):
        return await self.get_pool()
