from asyncio import gather
from asyncpg import create_pool, connect
from cached_property import cached_property

import copy
from jsondiff import diff

from .exceptions import NotIncluded
from .store import ParentStore, WithConfig
from .utils import get_include_query, get_version_number, confirm, aecho
from .model import Model
from .sql import print_query
from .namespace import Namespace

DATABASE_VERSION_QUERY = "SELECT version()"

sepn = f"\n{'=' * 40}"
sep = f'{sepn}\n'


class Database(WithConfig, ParentStore):
    child_key = "schemas"
    type = "db"

    def __init__(
        self,
        name=None,
        host=None,
        url=None,
        config=None,
        tag=None,
        verbose=False,
        prompt=False,
    ):
        self.config = config
        config_url = self.config.get('url')
        if config_url and not url:
            url = config_url

        if url and not host:
            from .host import Host

            host = Host(url)
            if not name:
                name = host.dbname

        self.prompt = prompt
        self.name = name
        self.parent = self.host = host
        self.verbose = verbose
        self.tag = tag
        self._schemas = {}
        self._connection = None
        self._models = {}

    def __str__(self):
        return self.name

    async def model(self, schema, table_name, refresh=False):
        key = (schema, table_name)
        if key not in self._models or refresh:
            namespace = None
            async for child in self.get_children(refresh=refresh):
                if child.name == schema:
                    namespace = child
                    break
            if not namespace:
                raise ValueError(f'schema {schema} not found or no access')

            table = None
            async for child in namespace.get_children(refresh=refresh):
                if child.name == table_name:
                    table = child
                    break
            if not table:
                raise ValueError(f'table {schema}.{table_name} not found or no access')

            self._models[key] = Model(table=table)
        return self._models[key]

    @cached_property
    async def full_version(self):
        return await self.get_full_version()

    async def stream(self, *query, transaction=True, connection=None):
        pool = await self.pool
        connection = aecho(connection) if connection else pool.acquire()
        async with connection as conn:
            transaction = conn.transaction() if transaction else aecho()
            async with transaction:
                async for row in conn.cursor(*query):
                    yield row

    def use(self, connection):
        self._connection = connection

    async def copy_from(self, **kwargs):
        pool = await self.pool
        table_name = kwargs.pop('table_name', None)
        transaction = kwargs.pop('transaction', False)
        connection = kwargs.pop('connection', self._connection)
        connection = aecho(connection) if connection else pool.acquire()
        close = kwargs.pop('close', False)
        query = kwargs.pop('query', None)
        async with connection as conn:
            transaction = conn.transaction() if transaction else aecho()
            async with transaction:
                result = None
                if table_name:
                    result = await conn.copy_from_table(table_name, **kwargs)
                elif query:
                    result = await conn.copy_from_query(*query, **kwargs)
                else:
                    raise NotImplementedError('table or query is required')
                if close:
                    output = kwargs.get('output')
                    if getattr(output, 'close'):
                        output.close()
                return result

    async def copy_to(self, **kwargs):
        pool = await self.pool
        table_name = kwargs.pop('table_name', None)
        transaction = kwargs.pop('transaction', False)
        connection = kwargs.pop('connection', None) or self._connection
        connection = aecho(connection) if connection else pool.acquire()
        async with connection as conn:
            transaction = conn.transaction() if transaction else aecho()
            async with transaction:
                if table_name:
                    return await conn.copy_to_table(table_name, **kwargs)
                else:
                    raise NotImplementedError('table is required')

    async def execute(self, *query, connection=None, transaction=False):
        pool = await self.pool
        pquery = print_query(query)
        connection = connection or self._connection
        connection = aecho(connection) if connection else pool.acquire()

        async with connection as conn:
            if self.prompt:
                if not confirm(
                    f"{sep}{pquery}{sepn}",
                    True,
                ):
                    raise Exception(f"{self}: execute aborted")
            elif self.verbose:
                print(f"{self}: execute{sep}{pquery}{sepn}")
            transaction = conn.transaction() if transaction else aecho()
            async with transaction:
                try:
                    return await conn.execute(*query)
                except Exception as e:
                    err = f"{self}: execute failed; {e.__class__.__name__}: {e}"
                    err += f"\nQuery:{sep}{pquery}{sepn}"
                    raise Exception(err)

    async def query(
        self, *query, connection=None, many=True, columns=True, transaction=False
    ):
        pool = await self.pool
        connection = connection or self._connection
        connection = aecho(connection) if connection else pool.acquire()
        pquery = print_query(query)

        async with connection as conn:
            if self.prompt:
                if not confirm(
                    f"{sep}{pquery}{sepn}",
                    True,
                ):
                    raise Exception(f"{self}: query aborted")
            elif self.verbose:
                print(f"{self}: query{sep}{pquery}{sepn}")
            transaction = conn.transaction() if transaction else aecho()
            async with transaction:
                try:
                    results = await conn.fetch(*query)
                except Exception as e:
                    err = f"{self}: query failed; {e.__class__.__name__}: {e}"
                    err += f"\nQuery:{sep}{pquery}{sepn}"
                    raise Exception(err)
                if many:
                    return results if columns else [r[0] for r in results]
                else:
                    num = len(results)
                    if num != 1:
                        raise Exception(
                            f"{self}: query failed; expecting 1 row, got {num}\n"
                            f"Query:{sep}{query}{sepn}"
                        )
                    result = results[0]
                    return result if columns else result[0]

    async def query_one_row(self, *query, as_=None, **kwargs):
        result = await self.query(*query, many=False, columns=True, **kwargs)
        if as_:
            return as_(result)
        return result

    async def query_one_column(self, *query, **kwargs):
        return await self.query(*query, many=True, columns=False, **kwargs)

    async def query_one_value(self, *query, **kwargs):
        return await self.query(*query, many=False, columns=False, **kwargs)

    async def get_full_version(self):
        version = await self.query_one_value(DATABASE_VERSION_QUERY)
        return version

    @cached_property
    async def version(self):
        return get_version_number(await self.full_version)

    def get_namespaces_query(self):
        table = "pg_namespace"
        column = "nspname"
        include = self.get_child_include()
        query, args = get_include_query(include, table, column)
        if query:
            query = "WHERE {}".format(query)
        args.insert(0, 'SELECT "{}"\nFROM "{}" {}'.format(column, table, query))
        return args

    def get_namespace(self, name, refresh=False):
        if name not in self._schemas or refresh:
            config = self.get_child_config(name)
            self._schemas[name] = Namespace(
                name, database=self, config=config, verbose=self.verbose, tag=self.tag
            )
        return self._schemas[name]

    async def diff(self, other, translate=None, only=None, info=False, refresh=False):
        self.log(f"{self}: diff")
        if only:
            assert only == "schema" or only == "data"

        data = self.get_info(only=only, refresh=refresh)
        other_data = other.get_info(only=only, refresh=refresh)
        data, other_data = await gather(data, other_data)
        original_data = data
        if translate:
            if info:
                original_data = copy.deepcopy(data)
            # translate after both diffs have already been captured
            schemas = translate.get("schemas", {})
            types = translate.get("types", {})
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
                        if "schema" not in table:
                            continue
                        for column in table["schema"]["columns"].values():
                            if column["type"] in types:
                                column["type"] = types[column["type"]]

        diff_data = diff(data, other_data, syntax="symmetric")
        return (original_data, other_data, diff_data) if info else diff_data

    async def get_pool(self):
        return await create_pool(dsn=self.host.url, max_size=20)

    async def get_connection(self):
        return await connect(self.host.url)

    async def get_children(self, refresh=False):
        query = self.get_namespaces_query()
        async for row in self.stream(*query):
            try:
                yield self.get_namespace(row[0], refresh=refresh)
            except NotIncluded:
                pass

    @cached_property
    async def namespaces(self):
        namespaces = {}
        async for child in self.get_children():
            namespaces[child.name] = child
        return namespaces

    @cached_property
    async def pool(self):
        return await self.get_pool()
