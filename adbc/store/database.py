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
from adbc.preql import build
from .namespace import Namespace

SKIP_CA_CHECK = os.environ.get('ADBC_SKIP_CA_CHECK') == '1'


class Database(Loggable, WithCopy, WithScope):
    child_key = "schemas"

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

    def __str__(self):
        return self.name

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

    async def get_model(self, table_name, schema=None, scope=None):
        scope = scope or self.scope

        if schema is None:
            schema = self.backend.default_schema

        key = (scope, schema, table_name)
        return await self.cache_by_async(
            'models',
            key,
            lambda: self._get_model(table_name, schema, scope=scope)
        )

    async def _get_model(self, table_name, schema=None, scope=None):
        table = await self.get_table(table_name, schema=schema, scope=scope)
        return Model(database=self, table=table)

    async def get_table(self, table_name, schema=None, scope=None):
        scope = scope or self.scope

        if isinstance(table_name, list):
            schema, table_name = table_name

        if schema is None:
            schema = self.backend.default_schema

        key = (scope, schema, table_name)
        return await self.cache_by_async(
            'tables',
            key,
            lambda: self._get_table(table_name, schema, scope)
        )

    async def _get_table(self, table_name, schema, scope):
        namespace = None
        children = await self.get_children(scope=scope)
        for child in children:
            if child.name == schema:
                namespace = child
                break

        if not namespace:
            raise ValueError(f"schema {schema} not found or no access")

        table = None
        children = await namespace.get_children()
        for child in children:
            if child.name == table_name:
                table = child
                break
        if not table:
            raise ValueError(f"table {schema}.{table_name} not found or no access")
        return table

    @cached_property
    async def is_redshift(self):
        version = await self.full_version
        return "redshift" in version.lower()

    @cached_property
    async def full_version(self):
        return await self.get_full_version()

    async def stream(self, query, params=None, transaction=True, connection=None):
        if isinstance(query, (dict, list)):
            # build PreQL
            queries = build(query, dialect=self.backend.dialect)
        else:
            queries = [(query, params)]

        pool = await self.pool
        connection = connection or self._connection
        connection = aecho(connection) if connection else pool.acquire()

        async with connection as conn:
            transaction = conn.transaction() if transaction else aecho()
            async with transaction:
                for query, params in queries:
                    pquery = print_query(query, params)
                    if self.prompt:
                        if not confirm(f"{self.name} ({self.tag}): {SEP}{pquery}{SEPN}", True):
                            raise Exception(f"{self}: stream aborted")
                    else:
                        # self.log(f"{self}: stream{SEP}{pquery}{SEPN}")
                        async for row in self.backend.cursor(conn, query, params):
                            yield row

    def use(self, connection):
        self._connection = connection

    async def execute(self, query, params=None, connection=None, transaction=False):
        if isinstance(query, (dict, list)):
            # build PreQL
            query, params = build(query, dialect=self.backend.dialect, combine=True)

        pool = await self.pool
        connection = connection or self._connection
        connection = aecho(connection) if connection else pool.acquire()
        pquery = print_query(query, params)

        async with connection as conn:
            if self.prompt:
                if not confirm(f"{self.name} ({self.tag}): {SEP}{pquery}{SEPN}", True):
                    raise Exception(f"{self}: execute aborted")
            else:
                self.log(f"{self}: execute{SEP}{pquery}{SEPN}")
            transaction = conn.transaction() if transaction else aecho()
            async with transaction:
                try:
                    return await self.backend.execute(
                        conn, query, params
                    )
                except Exception as e:
                    err = f"{self}: execute failed; {e.__class__.__name__}: {e}"
                    err += f"\nQuery:{SEP}{pquery}{SEPN}"
                    raise Exception(err)

    async def query(
        self, query, params=None, connection=None, many=True, columns=True, transaction=False
    ):
        pool = await self.pool
        connection = connection or self._connection
        connection = aecho(connection) if connection else pool.acquire()

        if isinstance(query, (dict, list)):
            # build PreQL
            queries = build(query, dialect=self.backend.dialect)
        else:
            queries = [(query, params)]

        one = len(queries) == 1
        all_results = []
        async with connection as conn:
            transaction = conn.transaction() if transaction else aecho()
            async with transaction:
                for query, params in queries:
                    pquery = print_query(query, params)
                    if self.prompt:
                        if not confirm(f"{self.name} ({self.tag}): {SEP}{pquery}{SEPN}", True):
                            raise Exception(f"{self}: query aborted")
                    else:
                        self.log(f"{self}: query{SEP}{pquery}{SEPN}")
                    try:
                        results = await self.backend.fetch(conn, query, params)
                    except Exception as e:
                        err = f"{self}: query failed; {e.__class__.__name__}: {e}"
                        err += f"\nQuery:{SEP}{pquery}{SEPN}"
                        raise Exception(err)
                    if many:
                        all_results.append(
                            results if columns else [r[0] for r in results]
                        )
                        continue
                    else:
                        num = len(results)
                        if num == 0:
                            # no results -> return None
                            all_results.append(None)
                            continue
                        if num != 1:
                            raise Exception(
                                f"{self}: query failed; expecting <=1 row, got {num}\n"
                                f"Query:{SEP}{query}{SEPN}"
                            )
                        result = results[0]
                        all_results.append(result if columns else result[0])

        return all_results[0] if one else all_results

    async def query_one_row(self, query, params=None, **kwargs):
        return await self.query(query, params=params, many=False, columns=True, **kwargs)

    async def query_one_column(self, query, params=None, **kwargs):
        return await self.query(query, params=params, many=True, columns=False, **kwargs)

    async def query_one_value(self, query, params=None, **kwargs):
        return await self.query(query, params=params, many=False, columns=False, **kwargs)

    async def get_full_version(self):
        version = await self.query_one_value(self.backend.get_query('version'))
        return version

    @cached_property
    async def version(self):
        return get_version_number(await self.full_version)

    def get_namespaces_query(self, scope=None):
        include = self.get_child_include(scope=scope)
        tag = self.tag
        return self.backend.get_query('namespaces', include, tag=tag)

    def get_namespace(self, name, scope=None):
        scope = scope or self.scope
        return self.cache_by(
            'schemas',
            {'name': name, 'scope': scope},
            lambda: self._get_namespace(name, scope)
        )

    def _get_namespace(self, name, scope):
        translation = self.get_scope_translation(scope=scope, from_=self.tag)
        scope = self.get_child_scope(name, scope=scope)
        alias = translation.get(name, name)
        return Namespace(
            name,
            database=self,
            scope=scope,
            alias=alias,
            verbose=self.verbose,
            tag=self.tag,
        )

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

    async def get_children(self, scope=None):
        scope = scope or self.scope
        return await self.cache_by_async(
            'children',
            scope,
            lambda: self.get_namespaces(scope=scope)
        )

    async def get_namespaces(self, scope=None):
        query = self.get_namespaces_query(scope=scope)
        rows = await self.query(query)
        result = []
        for row in rows:
            try:
                result.append(
                    self.get_namespace(row[0], scope=scope)
                )
            except NotIncluded:
                pass
        return result

    @property
    async def pool(self):
        if not getattr(self, '_pool', None):
            self._pool = await self.get_pool()
        return self._pool
