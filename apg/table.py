from .store import Store
from deepdiff import DeepHash
from cached_property import cached_property


def get_by_name(l, name, key):
    value = next((x for x in l if x["name"] == name), None)
    if value:
        return value[key]
    else:
        return None


class Table(Store):
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
        assert namespace
        self.name = name
        self.verbose = verbose
        self.namespace = namespace
        self.database = namespace.database
        self.attributes = list(sorted(attributes or [], key=lambda c: c["name"]))
        self.constraints = list(sorted(constraints or [], key=lambda c: c["name"]))
        self.indexes = list(sorted(indexes or [], key=lambda c: c["name"]))
        self.tag = tag
        self.pks = next(
            (
                get_by_name(self.indexes, c["index_name"], "keys")
                for c in self.constraints
                if c["type"] == "p"
            ),
            None,
        )

    async def get_diff_data(self):
        data_hash = self.get_data_hash()
        count = self.get_count()
        return {
            "data_hash": await data_hash,
            "count": await count,
            "schema": self.get_schema(),
        }

    def get_schema(self):
        return {
            "name": self.name,
            "attributes": self.attributes,
            "constraints": self.constraints,
            "indexes": self.indexes,
        }

    async def get_schema_hash(self):
        schema = self.get_schema()
        return DeepHash(schema)[schema]

    def get_data_hash_query(self):
        return [
            "SELECT md5(array_agg(md5((t.*)::varchar))::varchar)"
            "FROM (SELECT * FROM {}.{} ORDER BY {}) AS t".format(
                self.namespace.name,
                self.name,
                ", ".join(['"{}"'.format(x) for x in self.pks]),
            )
        ]

    def get_count_query(self):
        return ['SELECT COUNT(*) FROM "{}"."{}"'.format(self.namespace.name, self.name)]

    async def get_data_hash(self):
        pool = await self.database.pool
        query = self.get_data_hash_query()
        async with pool.acquire() as connection:
            result = await connection.fetchval(*query)
            self.print(
                "<- {}.table.{}.data_hash = {}".format(
                    self.tag or "", self.name, result
                )
            )
            return result

    async def get_count(self):
        pool = await self.database.pool
        query = self.get_count_query()
        async with pool.acquire() as connection:
            result = await connection.fetchval(*query)
            self.print(
                "<- {}.table.{}.count = {}".format(self.tag or "", self.name, result)
            )
            return result

    @cached_property
    async def count(self):
        return await self.get_count()
