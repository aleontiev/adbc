from .store import Store
from deepdiff import DeepHash
from cached_property import cached_property


class Table(Store):
    def __init__(
        self,
        name,
        namespace=None,
        attributes=None,
        constraints=None,
        indexes=None
    ):
        assert(namespace)
        self.name = name
        self.namespace = namespace
        self.database = namespace.database
        self.attributes = attributes
        self.constraints = constraints

    def schema_hash(self):
        return DeepHash({
            'name': self.name,
            'attributes': self.attributes,
            'constraints': self.constraints,
            'indexes': self.indexes
        })

    def get_count_query(self):
        return ['SELECT COUNT(1) FROM "{}"."{}"'.format(
            self.namespace.name,
            self.name
        )]

    async def get_count(self):
        with self.database.pool.acquire() as connection:
            return connection.fetchval(*self.get_count_query())

    @cached_property
    async def count(self):
        return self.get_count()

    @cached_property
    async def count_query(self):
        return self.get_count_query()
