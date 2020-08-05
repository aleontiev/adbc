import os
from cached_property import cached_property

from adbc.exceptions import NotIncluded
from adbc.scope import WithScope
from adbc.utils import get_version_number, confirm, aecho
from adbc.model import Model
from adbc.sql import print_query
from adbc.operations.copy import WithCopy
from adbc.logging import Loggable
from adbc.constants import SEP, SEPN
from .namespace import Namespace

SKIP_CA_CHECK = os.environ.get('ADBC_SKIP_CA_CHECK') == '1'


class Database(Loggable, WithCopy, WithScope):
    child_key = "schemas"
    type = "db"

    def __init__(
        self,
        name=None,
        host=None,
        url=None,
        scope=None,
        tag=None,
        alias=None,
        verbose=False,
        prompt=False,
        min_pool_size=5,
        max_pool_size=20,
        **kwargs
    ):
        super().__init__(**kwargs)

        self.scope = scope

        if url and not host:
            from .host import Host

            host = Host(url)
            if not name:
                name = host.dbname

        self.min_pool_size = min_pool_size
        self.max_pool_size = max_pool_size
        self.url = url
        self.prompt = prompt
        self.name = name
        self.alias = alias or name
        self.parent = self.host = host
        self.verbose = verbose
        self.tag = tag
        self._pool = None
        self._connection = None
        self._schemas = {}
        self._models = {}
        self._tables = {}

    def __str__(self):
        return self.name

    def clear_cache(self):
        self._schemas = {}
        self._models = {}
        self._tables = {}

    @cached_property
    async def shard_size(self):
        # TODO: support for other database backends
        is_redshift = await self.is_redshift
        return 1000 if is_redshift else 16000

    async def close(self):
        if self._pool:
            await self._pool.close()
            self._pool = None
        if self._connection:
            await self._connection.close()
            self._connection = None

    async def get_model(self, table_name, schema=None, refresh=False):
        if isinstance(table_name, dict):
            schema = table_name.get('schema', schema)
            table_name = table_name['table']

        if schema is None:
            schema = self.backend.default_schema

        key = (schema, table_name)
        if key not in self._models or refresh:
            table = await self.get_table(table_name, refresh=refresh, schema=schema)
            self._models[key] = Model(database=self, table=table)
        return self._models[key]

    async def get_table(self, table_name, schema=None, refresh=False):
        if isinstance(table_name, dict):
            schema = table_name.get('schema', schema)
            table_name = table_name['table']

        if schema is None:
            schema = self.backend.default_schema

        key = (schema, table_name)
        if key not in self._tables or refresh:
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

            self._tables[key] = table
        return self._tables[key]

    @cached_property
    async def is_redshift(self):
        version = await self.full_version
        return "redshift" in version.lower()

    @cached_property
    async def full_version(self):
        return await self.get_full_version()

    async def stream(self, *query, transaction=True, connection=None):
        pool = await self.pool
        connection = aecho(connection) if connection else pool.acquire()
        pquery = print_query(query)
        async with connection as conn:
            if self.prompt:
                if not confirm(f"{self.name} ({self.tag}): {SEP}{pquery}{SEPN}", True):
                    raise Exception(f"{self}: stream aborted")
            else:
                self.log(f"{self}: stream{SEP}{pquery}{SEPN}")
            transaction = conn.transaction() if transaction else aecho()
            async with transaction:
                async for row in conn.cursor(*query):
                    yield row

    def use(self, connection):
        self._connection = connection

    async def execute(self, *query, connection=None, transaction=False):
        pool = await self.pool
        connection = connection or self._connection
        connection = aecho(connection) if connection else pool.acquire()
        pquery = print_query(query)

        async with connection as conn:
            if self.prompt:
                if not confirm(f"{self.name} ({self.tag}): {SEP}{pquery}{SEPN}", True):
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
                if not confirm(f"{self.name} ({self.tag}): {SEP}{pquery}{SEPN}", True):
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
        # preql = {"select": {"values": {"version": {"version": []}}}
        version = await self.query_one_value(*self.backend.get_query('version'))
        return version

    @cached_property
    async def version(self):
        return get_version_number(await self.full_version)

    def get_namespaces_query(self, scope=None):
        include = self.get_child_include(scope=scope)
        tag = self.tag
        return self.backend.get_query('namespaces', include, tag=tag)

    def get_namespace(self, name, scope=None, refresh=False):
        if name not in self._schemas or refresh:
            translation = self.get_scope_translation(scope=scope, from_=self.tag)
            scope = self.get_child_scope(name, scope=scope)
            alias = translation.get(name, name)
            self._schemas[name] = Namespace(
                name,
                database=self,
                scope=scope,
                alias=alias,
                verbose=self.verbose,
                tag=self.tag,
            )
        return self._schemas[name]

    @property
    def F(self):
        return self.backend.F

    @cached_property
    def backend(self):
        return self.host._backend

    async def get_pool(self):
        return await self.backend.create_pool(
            dsn=self.host.url,
            max_size=self.max_pool_size,
            min_size=self.min_pool_size,
            skip_ca_check=SKIP_CA_CHECK
        )

    async def get_connection(self):
        return await self.backend.connect(self.host.url)

    async def get_children(self, scope=None, refresh=False):
        query = self.get_namespaces_query(scope=scope)
        async for row in self.stream(*query):
            try:
                yield self.get_namespace(row[0], scope=scope, refresh=refresh)
            except NotIncluded:
                pass

    @property
    async def pool(self):
        if not getattr(self, '_pool', None):
            self._pool = await self.get_pool()
        return self._pool
