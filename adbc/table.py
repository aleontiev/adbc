import asyncio
from .store import Store
from cached_property import cached_property


def get_first(items, fn, then=None):
    if isinstance(items, dict):
        items = items.values()

    for item in items:
        if fn(item):
            return item[then] if then else item
    return None


def split_field(i, f):
    for key in i:
        value = key.pop(f, None)
        yield (value, key)


class Table(Store):
    type = "table"

    def __init__(
        self,
        name,
        config=None,
        namespace=None,
        attributes=None,
        constraints=None,
        indexes=None,
        verbose=False,
        tag=None,
    ):
        if not isinstance(config, dict):
            self.config = {}
        else:
            self.config = config

        self.name = name
        self.verbose = verbose
        self.parent = self.namespace = namespace
        self.database = namespace.database
        self.attributes = {
            k: v
            for k, v in split_field(
                sorted(attributes or [], key=lambda c: c["name"]), "name"
            )
        }
        self.columns = list(self.attributes.keys())
        self.constraints = {
            k: v
            for k, v in split_field(
                sorted(constraints or [], key=lambda c: c["name"]), "name"
            )
        }
        if not self.config.get('indexes', True):
            self.indexes = {}
        else:
            self.indexes = {
                k: v
                for k, v in split_field(
                    sorted(indexes or [], key=lambda c: c["name"]), "name"
                )
            }

        self.tag = tag
        self.pks = []
        if self.indexes:
            self.pks = get_first(
                self.indexes,
                lambda item: item["primary"],
                "attributes"
            )

        if not self.pks and self.constraints:
            self.pks = get_first(
                self.constraints,
                lambda item: item['type'] == 'p',
                'attributes'
            )

        if not self.pks:
            # full-row pks
            self.pks = self.columns

        if not self.config.get('constraints', True):
            # if constraints are disabled, we still try to read the pk fields
            # and only then wipe the field
            self.constraints = {}

    async def get_diff_data(self):
        data_range = self.get_data_range()
        data_hash = self.get_data_hash()
        count = self.get_count()
        schema = self.get_schema()
        data_range, data_hash, count = await asyncio.gather(
            data_range, data_hash, count
        )
        return {
            "data": {
                "hash": data_hash,
                "count": count,
                "range": data_range,
            },
            "schema": schema,
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
        if version < "9":
            # TODO: fix, technically this applies to Redshift, not Postgres <9
            # in practice, nobody else is running Postgres 8 anymore...
            aggregator = "listagg"
            end = ", ',')) "
        else:
            aggregator = "array_to_string(array_agg"
            end = "), ',')) "

        # concatenate all column names and values in pseudo-json
        aggregate = " || ".join([f"'{c}' || " f'T."{c}"' for c in self.columns])
        order = ", ".join([f'"{c}"' for c in self.pks])
        namespace = self.namespace.name
        return [
            f"SELECT md5({aggregator}({aggregate}{end}"
            f"FROM (SELECT * FROM {namespace}.{self.name} ORDER BY {order}) AS T"
        ]

    def get_count_query(self):
        return ['SELECT COUNT(*) FROM "{}"."{}"'.format(self.namespace.name, self.name)]

    async def get_data_range_query(self):
        pks = self.pks
        if len(pks) == 1:
            pk = pks[0]
            return [
                f'SELECT MIN("{pk}") as "from", MAX("{pk}") as "to" '
                f'FROM "{self.namespace.name}"."{self.name}"'
            ]
        else:
            version = await self.database.version
            aggregator = "array_agg"
            if version < "9":
                aggregator = "listagg"
            pks = " || '/' || ".join([f'"{pk}"' for pk in pks])
            return [
                f"SELECT MIN(T.pks) as \"from\", MAX(T.pks) as \"to\" FROM"
                f'(SELECT {aggregator}({pks}) as pks '
                f'FROM "{self.namespace.name}"."{self.name}") AS T'
            ]

    async def get_data_range(self):
        query = await self.get_data_range_query()
        return await self.database.query_one_row(*query, as_=dict)

    async def get_data_hash(self):
        query = await self.get_data_hash_query()
        return await self.database.query_one_value(*query)

    async def get_count(self):
        query = self.get_count_query()
        return await self.database.query_one_value(*query)

    @cached_property
    async def count(self):
        return self.get_count()
