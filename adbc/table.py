import asyncio
from copy import copy
from collections import defaultdict
from .store import Store
from adbc.sql import column
from cached_property import cached_property
from adbc.utils import get_first, split_field


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


def can_order(type, uuid=True):
    if type in NO_ORDER_TYPES:
        return False
    if uuid is False:
        if type == 'uuid':
            return False
    return True


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
        self.on_create = self.config.get('on_create', None)
        self.on_update = self.config.get('on_update', None)
        self.on_delete = self.config.get('on_update', None)
        self.immutable = self.config.get(
            'immutable', not (bool(self.on_update) or bool(self.on_delete))
        )
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

        self.pks = Table.get_pks(
            self.indexes,
            self.constraints,
            self.column_names
        )

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

        self.log(f'init: {self}')

    @staticmethod
    def get_pks(indexes, constraints, columns):
        pks = None
        if indexes:
            pks = get_first(
                indexes,
                lambda item: item["primary"],
                "columns"
            )

        if not pks and constraints:
            pks = get_first(
                constraints,
                lambda item: item['type'] == 'p',
                'columns'
            )

        if not pks:
            # full-row pks
            pks = columns
        return pks

    def __str__(self):
        return f'{self.namespace}.{self.name}'

    async def get_info(self, only=None):
        result = {}
        if only == 'data' or not only:
            data_range = self.get_data_range()
            # data_hash = self.get_data_hash()
            count = self.get_count()
            data_range, count = await asyncio.gather(
                data_range, count
            )
            result['data'] = {
                # 'hash': data_hash,
                'count': count,
                'range': data_range
            }
        if only == 'schema' or not only:
            result['schema'] = self.get_schema()

        self.log(f"info: {self}")
        return result

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
        cast = False
        if version < 9:
            # TODO: fix, technically this applies to Redshift, not Postgres <9
            # in practice, nobody else is running Postgres 8 anymore...
            aggregator = "listagg"
            end = ", ',')"
            decode = True
            cast = True

        columns = self.columns

        cast = '::varchar' if cast else ''

        # concatenate all column names and values in pseudo-json
        aggregate = " || '' || \n ".join([
            f'T.{column(c)}{cast}'
            for c in self.columns
        ])
        pks = self.pks
        namespace = self.namespace.name
        order = ",\n    ".join([
            column(c) for c in pks if self.can_order(c)
        ])
        cols = ",\n    ".join([
            self.get_decode_boolean(c) if decode and self.is_boolean(c) else (
                column(c) if not self.is_array(c)
                else f'array_to_string({column(c)}, \',\') as {c}'
            )
            for c in columns
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

    def can_order(self, column_name, uuid=True):
        column = self.columns[column_name]
        return can_order(column['type'], uuid=uuid)

    def is_boolean(self, column_name):
        column = self.columns[column_name]
        type = column['type']
        return type == 'boolean'

    def is_array(self, name):
        column = self.columns[name]
        type = column['type']
        return '[]' in type or 'idvector' in type or 'intvector' in type

    def is_short(self, name):
        column = self.columns[name]
        return column['type'] != 'pg_node_tree'

    def is_uuid(self, name):
        column = self.columns[name]
        return column['type'] != 'uuid'

    def get_count_query(self):
        return (f'SELECT COUNT(*) FROM "{self.namespace.name}"."{self.name}"', )

    async def get_data_range_query(self, keys):
        new_keys = []
        for key in keys:
            new_keys.append(
                f'MIN({column(key)}) AS "min_{key}", '
                f'MAX({column(key)}) AS "max_{key}"'
            )
        keys = ',\n  '.join(new_keys)
        return (
            f'SELECT {keys}\n'
            f'FROM "{self.namespace.name}"."{self.name}"',
        )

    async def get_data_range(self):
        keys = copy(self.pks) if len(self.pks) == 1 else []
        keys = [key for key in keys if self.can_order(key, uuid=False)]
        if self.on_create:
            keys.append(self.on_create)
        if self.on_update:
            keys.append(self.on_update)

        if not keys:
            return None

        keys = list(set(keys))
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
        if self.namespace.name in INTERNAL_SCHEMAS and version < 9:
            return None
        query = await self.get_data_hash_query()
        return await self.database.query_one_value(*query)

    async def get_count(self):
        query = self.get_count_query()
        return await self.database.query_one_value(*query)

    @cached_property
    async def count(self):
        return self.get_count()
