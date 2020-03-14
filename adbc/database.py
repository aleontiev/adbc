from asyncio import gather
from asyncpg import create_pool
from cached_property import cached_property

from jsondiff import diff

from .exceptions import NotIncluded
from .store import ParentStore, WithConfig
from .utils import get_include_query, get_server_version
from .utils import confirm
from .namespace import Namespace

DATABASE_VERSION_QUERY = "SELECT version()"


class Database(WithConfig, ParentStore):
    child_key = "schemas"
    type = "db"

    def __init__(
        self, name=None, host=None, url=None, config=None, tag=None, verbose=False
    ):
        if url and not host:
            from .host import Host

            host = Host(url)
            if not name:
                name = host.dbname

        self.prompt_execute = True
        self.name = name
        self.parent = self.host = host
        self.config = config
        self.verbose = verbose
        self.tag = tag
        self.log(f'init: {self}')

    def __str__(self):
        return self.name

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
        sep = '=' * 10
        print_query = query
        if len(print_query) == 1:
            print_query = print_query[0]

        async with pool.acquire() as connection:
            async with connection.transaction():
                if self.prompt_execute:
                    if not confirm(
                        f"Run query on DB {self.name}?"
                        f"\n{sep}\n{print_query}\n{sep}\n",
                        True,
                    ):
                        raise Exception('Aborted')
                try:
                    return await connection.execute(*query)
                except Exception as e:
                    err = f"query failed with error:" f"{e.__class__}: {e}"
                    if not self.prompt_execute:
                        err += f"\nQuery:\n{sep}\n{print_query}\n{sep}\n"
                    raise Exception(err)

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
                        f"query failed:\n======\n{query}\n======\n"
                        f"{e.__class__}: {e}"
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
        return result

    async def query_one_value(self, *query):
        return await self.query(*query, many=False, columns=False)

    async def get_version(self):
        version = await self.query_one_value(DATABASE_VERSION_QUERY)
        return get_server_version(version)

    def get_namespaces_query(self):
        table = "pg_namespace"
        column = "nspname"
        include = self.get_child_include()
        query, args = get_include_query(include, table, column)
        if query:
            query = "WHERE {}".format(query)
        args.insert(0, 'SELECT "{}"\nFROM "{}" {}'.format(column, table, query))
        return args

    def get_namespace(self, name):
        config = self.get_child_config(name)
        return Namespace(
            name, database=self, config=config, verbose=self.verbose, tag=self.tag
        )

    async def diff(self, other, translate=None, only=None):
        self.log(f"diff: {self}")
        if only:
            assert(only == 'schema' or only == 'data')

        data = self.get_info(only=only)
        other_data = other.get_info(only=only)
        data, other_data = await gather(data, other_data)

        if translate:
            # translate after both diffs have already been captured
            schemas = translate.get('schemas', {})
            types = translate.get('types', {})
            # table/schema names
            for key, value in schemas.items():
                if key == value:
                    continue

                # source schema "key" is the same as target schema "value"
                if key in data:
                    data[value] = data[key]
                    data.pop(key)
            # column typesa
            if types:
                types = {k: v for k, v in types.items() if k != v}
            if types:
                # iterate over all columns and change type as appropriate
                for tables in data.values():
                    for table in tables.values():
                        if 'schema' not in table:
                            continue
                        for column in table['schema']['columns'].values():
                            if column['type'] in types:
                                column['type'] = types[column['type']]

        return diff(data, other_data, syntax="symmetric")

    async def get_pool(self):
        return await create_pool(dsn=self.host.url, max_size=20)

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
