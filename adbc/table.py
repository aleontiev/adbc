import asyncio
from copy import copy
from collections import defaultdict
from .store import Store
from cached_property import cached_property


NO_ORDER_TYPES = {
    'xid',
    'anyarray',
}
INTERNAL_SCHEMAS = {
    'pg_catalog',
    'information_schema'
}
SEQUENCE_TYPES = {
    'integer',
    'int4',
    'int8',
    'bigint',
    'int',
    'real',
    'date'
}


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
        columns=None,
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
        self.sequencer = self.config.get('sequencer', None)
        self.immutable = self.config.get('immutable', False)
        self.columns = {
            k: v
            for k, v in split_field(
                sorted(columns or [], key=lambda c: c["name"]), "name"
            )
        }

        if not self.config.get('sequences', True):
            # ignore nextval / sequence-based default values
            for column in self.columns.values():
                default = column.get('default', None)
                if isinstance(default, str) and default.startswith("nextval("):
                    column['default'] = None

        self.column_names = list(self.columns.keys())
        self.constraints = {
            k: v
            for k, v in split_field(
                sorted(constraints or [], key=lambda c: c["name"]), "name"
            )
        }
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
                "columns"
            )

        if not self.pks and self.constraints:
            self.pks = get_first(
                self.constraints,
                lambda item: item['type'] == 'p',
                'columns'
            )

        if not self.pks:
            # full-row pks
            self.pks = self.column_names

        if (
            not self.sequencer and
            len(self.pks) == 1 and
            self.columns[self.pks[0]]['type'] in SEQUENCE_TYPES
        ):
            self.sequencer = self.pks[0]

        # if disabled, remove constraints/indexes
        # but only after they are used to determine possible primary key
        constraints = self.config.get('constraints', True)
        if not constraints:
            self.constraints = None
        elif isinstance(constraints, str):
            self.constraints = {
                k: v
                for k, v in self.constraints.items()
                if v['type'] in constraints
            }

        if not self.config.get('indexes', True):
            self.indexes = None

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
        result = {
            "name": self.name,
            "columns": self.columns,
        }
        if self.constraints is not None:
            result['constraints'] = self.constraints
        if self.indexes is not None:
            result['indexes'] = self.indexes
        return result

    def get_decode_boolean(self, column):
        return f'decode("{column}", true, \'true\', false, \'false\') as "{column}"'

    async def get_data_hash_query(self):
        version = await self.database.version
        decode = False
        aggregator = "array_to_string(array_agg"
        end = "), ',')"
        if version < "9":
            # TODO: fix, technically this applies to Redshift, not Postgres <9
            # in practice, nobody else is running Postgres 8 anymore...
            aggregator = "listagg"
            end = ", ',')"
            decode = True

        columns = self.columns

        # concatenate all column names and values in pseudo-json
        aggregate = " ||\n ".join([
            (f"'{c}:' || " f'T."{c}"::varchar')
            for c in self.columns
        ])
        pks = self.pks
        namespace = self.namespace.name
        order = ",\n    ".join([
            f'"{c}"' for c in pks if self.can_order(c)
        ])
        cols = ",\n    ".join([
            self.get_decode_boolean(column) if decode and self.is_boolean(column) else (
                f'"{column}"' if not self.is_array(column)
                else f'array_to_string("{column}", \',\') as {column}'
            )
            for column in columns
        ])
        return [
            f"SELECT MD5(\n"
            f'  {aggregator}({aggregate}{end}\n'
            f')\n'
            f'FROM (\n'
            f'  SELECT {cols}\n'
            f'  FROM "{namespace}"."{self.name}"\n'
            f'  ORDER BY {order}'
            f') AS T'
        ]

    def can_order(self, column_name):
        column = self.columns[column_name]
        return column['type'] not in NO_ORDER_TYPES

    def is_boolean(self, column_name):
        column = self.columns[column_name]
        type = column['type']
        return type == 'boolean'

    def is_array(self, column_name):
        column = self.columns[column_name]
        type = column['type']
        return '[]' in type or 'vector' in type

    def is_short_column(self, column_name):
        column = self.columns[column_name]
        return column['type'] != 'pg_node_tree'

    def get_count_query(self):
        return (f'SELECT COUNT(*) FROM "{self.namespace.name}"."{self.name}"', )

    async def get_data_range_query(self, keys):
        keys = ',\n  '.join([
            f'MIN("{key}") AS "min_{key}", MAX("{key}") as "max_{key}"'
            for key in keys
        ])
        return (
            f'SELECT {keys}\n'
            f'FROM "{self.namespace.name}"."{self.name}"',
        )

    async def get_data_range(self):
        if len(self.pks) > 1:
            return None

        keys = copy(self.pks)
        if self.sequencer:
            keys.append(self.sequencer)
        query = await self.get_data_range_query(keys)
        row = await self.database.query_one_row(*query, as_=dict)
        result = defaultdict(dict)
        for key, value in row.items():
            type = key[0:3]
            key = key[4:]
            result[key][type] = value
        return result

    async def get_data_hash(self):
        version = await self.database.version
        if self.namespace.name in INTERNAL_SCHEMAS and version < '9':
            return None
        query = await self.get_data_hash_query()
        return await self.database.query_one_value(*query)

    async def get_count(self):
        query = self.get_count_query()
        return await self.database.query_one_value(*query)

    @cached_property
    async def count(self):
        return self.get_count()
