import asyncio
from .store import Store
from cached_property import cached_property


def get_first(items, fn, then=None):
    for item in items:
        if fn(item):
            return item[then] if then else item
    return None


class Table(Store):
    type = 'table'

    def __init__(
        self,
        name,
        namespace=None,
        attributes=None,
        constraints=None,
        indexes=None,
        verbose=False,
        tag=None,
    ):
        self.name = name
        self.verbose = verbose
        self.parent = self.namespace = namespace
        self.database = namespace.database
        self.attributes = list(sorted(attributes or [], key=lambda c: c["name"]))
        self.columns = [c['name'] for c in self.attributes]
        self.constraints = list(sorted(constraints or [], key=lambda c: c["name"]))
        self.indexes = list(sorted(indexes or [], key=lambda c: c["name"]))
        self.tag = tag
        self.pks = get_first(self.indexes, lambda item: item['primary'], 'columns')
        if not self.pks:
            # full-row pks
            self.pks = self.columns

    async def get_diff_data(self):
        data_hash = self.get_data_hash()
        count = self.get_count()
        schema = self.get_schema()
        data_hash, count = await asyncio.gather(data_hash, count)
        return {
            "hash": data_hash,
            "count": count,
            "schema": schema
        }

    def get_schema(self):
        return {
            "name": self.name,
            "attributes": self.attributes,
            "constraints": self.constraints,
            "indexes": self.indexes,
        }

    async def get_data_hash_query(self):
        version = await self.database.version
        if version < '9':
            # TODO: fix, technically this applies to Redshift, not Postgres <9
            # in practice, nobody else is running Postgres 8 anymore...
            aggregator = 'listagg'
            end = ", ',')) "
        else:
            aggregator = 'array_to_string(array_agg'
            end = "), ',')) "

        # concatenate all column names and values in pseudo-json
        aggregate = " || ".join([
            f"'{c}' || " f'T."{c}"' for c in self.columns
        ])
        order = ", ".join([
            f'"{c}"' for c in self.pks
        ])
        namespace = self.namespace.name
        return [
            f"SELECT md5({aggregator}({aggregate}{end}"
            f"FROM (SELECT * FROM {namespace}.{self.name} ORDER BY {order}) AS T"
        ]

    def get_count_query(self):
        return ['SELECT COUNT(*) FROM "{}"."{}"'.format(self.namespace.name, self.name)]

    async def get_data_hash(self):
        pool = await self.database.pool
        query = await self.get_data_hash_query()
        async with pool.acquire() as connection:
            return await connection.fetchval(*query)

    async def get_count(self):
        pool = await self.database.pool
        query = self.get_count_query()
        async with pool.acquire() as connection:
            return await connection.fetchval(*query)

    @cached_property
    async def count(self):
        return self.get_count()
