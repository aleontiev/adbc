from cached_property import cached_property

from .exceptions import NotIncluded
from .store import ParentStore, WithConfig
from .utils import get_version_number, confirm, aecho
from .model import Model
from .sql import print_query, get_tagged_number
from .namespace import Namespace

from .copy import WithCopy

SEPN = f"\n{'=' * 40}"
SEP = f"{SEPN}\n"


class Database(WithCopy, WithConfig, ParentStore):
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
        **kwargs
    ):
        super().__init__(**kwargs)

        self.config = config
        config_url = self.config.get("url")
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
                raise ValueError(f"schema {schema} not found or no access")

            table = None
            async for child in namespace.get_children(refresh=refresh):
                if child.name == table_name:
                    table = child
                    break
            if not table:
                raise ValueError(f"table {schema}.{table_name} not found or no access")

            self._models[key] = Model(table=table)
        return self._models[key]

    @cached_property
    async def is_redshift(self):
        version = await self.full_Version
        return "redshift" in version.lower()

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
        table_name = kwargs.pop("table_name", None)
        transaction = kwargs.pop("transaction", False)
        connection = kwargs.pop("connection", self._connection)
        connection = aecho(connection) if connection else pool.acquire()
        close = kwargs.pop("close", False)
        query = kwargs.pop("query", None)
        async with connection as conn:
            transaction = conn.transaction() if transaction else aecho()
            async with transaction:
                result = None
                if table_name:
                    self.log(f"{self}: copy from {table_name}")
                    result = get_tagged_number(
                        await conn.copy_from_table(table_name, **kwargs)
                    )
                elif query:
                    self.log(f"{self}: copy from {SEP}{print_query(query)}{SEPN}")
                    result = get_tagged_number(
                        await conn.copy_from_query(*query, **kwargs)
                    )
                else:
                    raise NotImplementedError("table or query is required")
                if close:
                    if hasattr(close, 'close'):
                        # close passed in object
                        output = close
                    else:
                        # close output object
                        output = kwargs.get("output")

                    if getattr(output, "close"):
                        output.close()
                return result

    async def copy_to(self, **kwargs):
        pool = await self.pool
        table_name = kwargs.pop("table_name", None)
        transaction = kwargs.pop("transaction", False)
        connection = kwargs.pop("connection", None) or self._connection
        connection = aecho(connection) if connection else pool.acquire()
        async with connection as conn:
            transaction = conn.transaction() if transaction else aecho()
            async with transaction:
                if table_name:
                    self.log(f"{self}: copy to {table_name}")
                    return get_tagged_number(
                        await conn.copy_to_table(table_name, **kwargs)
                    )
                else:
                    raise NotImplementedError("table is required")

    async def execute(self, *query, connection=None, transaction=False):
        pool = await self.pool
        connection = connection or self._connection
        connection = aecho(connection) if connection else pool.acquire()
        pquery = print_query(query)

        async with connection as conn:
            if self.prompt:
                if not confirm(f"{SEP}{pquery}{SEPN}", True):
                    raise Exception(f"{self}: execute aborted")
            else:
                self.log(f"{self}: execute{SEP}{pquery}{SEPN}")
            transaction = conn.transaction() if transaction else aecho()
            async with transaction:
                try:
                    return await conn.execute(*query)
                except Exception as e:
                    err = f"{self}: execute failed; {e.__class__.__name__}: {e}"
                    err += f"\nQuery:{SEP}{pquery}{SEPN}"
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
                if not confirm(f"{SEP}{pquery}{SEPN}", True):
                    raise Exception(f"{self}: query aborted")
            else:
                self.log(f"{self}: query{SEP}{pquery}{SEPN}")
            transaction = conn.transaction() if transaction else aecho()
            async with transaction:
                try:
                    results = await conn.fetch(*query)
                except Exception as e:
                    err = f"{self}: query failed; {e.__class__.__name__}: {e}"
                    err += f"\nQuery:{SEP}{pquery}{SEPN}"
                    raise Exception(err)
                if many:
                    return results if columns else [r[0] for r in results]
                else:
                    num = len(results)
                    if num == 0:
                        # no results -> return None
                        return None
                    if num != 1:
                        raise Exception(
                            f"{self}: query failed; expecting <=1 row, got {num}\n"
                            f"Query:{SEP}{query}{SEPN}"
                        )
                    result = results[0]
                    return result if columns else result[0]

    async def query_one_row(self, *query, **kwargs):
        return await self.query(*query, many=False, columns=True, **kwargs)

    async def query_one_column(self, *query, **kwargs):
        return await self.query(*query, many=True, columns=False, **kwargs)

    async def query_one_value(self, *query, **kwargs):
        return await self.query(*query, many=False, columns=False, **kwargs)

    async def get_full_version(self):
        version = await self.query_one_value(*self.backend.get_query('version'))
        return version

    @cached_property
    async def version(self):
        return get_version_number(await self.full_version)

    def get_namespaces_query(self):
        include = self.get_child_include()
        return self.host._backend.get_namespaces_query(self, include)

    def get_namespace(self, name, refresh=False):
        if name not in self._schemas or refresh:
            config = self.get_child_config(name)
            self._schemas[name] = Namespace(
                name,
                database=self,
                config=config,
                verbose=self.verbose,
                tag=self.tag
            )
        return self._schemas[name]

    @cached_property
    def backend(self):
        return self.host._backend

    async def get_pool(self):
        return await self.backend.create_pool(dsn=self.host.url, max_size=20)

    async def get_connection(self):
        return await self.backend.connect(self.host.url)

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
