import asyncio
from copy import copy
from collections import defaultdict
from adbc.logging import Loggable
from adbc.sql import format_column, format_table, get_pks, can_order
from cached_property import cached_property
from adbc.utils import split_field


class Table(Loggable):
    type = "table"

    def __init__(
        self,
        name,
        scope=None,
        namespace=None,
        columns=None,
        constraints=None,
        indexes=None,
        verbose=False,
        tag=None,
        **kwargs
    ):
        super().__init__(**kwargs)
        if not isinstance(scope, dict):
            self.scope = {}
        else:
            self.scope = scope

        self.name = name
        self.verbose = verbose
        self.parent = self.namespace = namespace
        self.database = namespace.database
        self.tag_name = self.scope.get(tag, name)
        self.on_create = self.scope.get("on_create", None)
        self.on_update = self.scope.get("on_update", None)
        self.on_delete = self.scope.get("on_update", None)
        self.immutable = self.scope.get(
            "immutable", not (bool(self.on_update) or bool(self.on_delete))
        )
        self.columns = {
            k: v
            for k, v in split_field(
                sorted(columns or [], key=lambda c: c["name"]), "name"
            )
        }

        if not self.scope.get("sequences", True):
            # ignore nextval / sequence-based default values
            for column in self.columns.values():
                if 'default' in column:
                    default = column['default']
                    if isinstance(default, str) and default.startswith("nextval("):
                        column["default"] = None

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

        self.pks = get_pks(self.indexes, self.constraints, self.column_names)

        # if disabled, remove constraints/indexes
        # but only after they are used to determine possible primary key
        constraints = self.scope.get("constraints", True)
        if not constraints:
            self.constraints = None
        elif isinstance(constraints, str):
            self.constraints = {
                k: v for k, v in self.constraints.items() if v["type"] in constraints
            }

        if not self.scope.get("indexes", True):
            self.indexes = None

    def __str__(self):
        return f"{self.namespace}.{self.name}"

    async def get_info(self, only=None, **kwargs):
        result = {}
        if only == "data" or not only:
            data_range = self.get_data_range()
            # data_hash = self.get_data_hash()
            count = self.get_count()
            data_range, count = await asyncio.gather(data_range, count)
            result["data"] = {
                # 'hash': data_hash,
                "count": count,
                "range": data_range,
            }
        if only == "schema" or not only:
            result["schema"] = self.get_schema()

        self.log(f"{self}: info")
        return result

    def get_schema(self):
        result = {"name": self.tag_name, "columns": self.columns}
        if self.constraints is not None:
            result["constraints"] = self.constraints
        if self.indexes is not None:
            result["indexes"] = self.indexes
        return result

    def get_decode_boolean(self, column):
        return f"decode(\"{column}\", true, 'true', false, 'false') as \"{column}\""

    async def get_statistics(self, **kwargs):
        query = await self.get_statistics_query(**kwargs)
        return await self.database.query_one_row(*query)

    async def get_statistics_query(
        self,
        count=False,
        max_pk=False,
        min_pk=False,
        md5=False,
        limit=None,
        cursor=None,
    ):
        if not count and not max_pk and not md5 and not min_pk:
            raise Exception("must pass count or max_pk or md5 or min_pk")

        redshift = await self.database.is_redshift
        decode = False
        aggregator = "array_to_string(array_agg"
        end = "), ',')"
        cast = False
        if redshift:
            # TODO: fix, technically this applies to Redshift, not Postgres <9
            # in practice, nobody else is running Postgres 8 anymore...
            aggregator = "listagg"
            end = ", ',')"
            decode = True
            cast = True

        columns = self.columns
        pks = self.pks
        order = ",\n    ".join([format_column(c) for c in pks if self.can_order(c)])
        cast = "::varchar" if cast else ""

        # concatenate all column names and values in pseudo-json
        aggregate = " || '' || \n ".join(
            [f"T.{format_column(c)}{cast}" for c in self.columns]
        )
        if not md5:
            columns = pks
        inner = ",\n    ".join(
            [
                self.get_decode_boolean(c)
                if decode and self.is_boolean(c)
                else (
                    format_column(c)
                    if not self.is_array(c)
                    else f"array_to_string({format_column(c)}, ',') as {c}"
                )
                for c in columns
            ]
        )
        output = []
        pk = pks[0]
        md5 = f"MD5(\n  {aggregator}({aggregate}{end})" if md5 else None
        count = "COUNT(*)" if count else ""
        max_pk = f"MAX({format_column(pk)})" if max_pk else ""
        min_pk = f"MIN({format_column(pk)})" if min_pk else ""
        if md5:
            output.append(md5)
        if count:
            output.append(count)
        if max_pk:
            output.append(max_pk)
        if min_pk:
            output.append(min_pk)

        if limit:
            limit = f"\n  LIMIT {int(limit)}\n"
        else:
            limit = ""

        where = ""
        if cursor:
            where = f"\n  WHERE {format_column(pk)} > $1\n"

        output = ", ".join(output)
        query = [
            f"SELECT {output}\n"
            f"FROM (\n"
            f"  SELECT {inner}\n"
            f"  FROM {self.sql_name}{where}\n"
            f"  ORDER BY {order}{limit}"
            f") AS T"
        ]
        if cursor:
            query.append(cursor)
        return query

    def get_min_id_query(self, limit=None, cursor=None, pk=None):
        if pk is None:
            pk = self.pks[0]

        pk = format_column(pk)
        if limit is None and cursor is None:
            return [f"SELECT {pk} FROM {self.sql_name} ORDER BY {pk} LIMIT 1"]

        where = ""
        if cursor:
            where = f"\n  WHERE {pk} > $1\n"
        if limit:
            limit = f"\n  LIMIT {int(limit)}\n"
        query = [
            f"SELECT T.{pk}\n"
            f"FROM (\n"
            f"  SELECT {pk}\n"
            f"  FROM {self.sql_name}{where}\n"
            f"  ORDER BY {pk}{limit}"
            f") AS T LIMIT 1"
        ]
        if cursor:
            query.append(cursor)
        return query

    def get_max_id_query(self, limit=None, cursor=None, pk=None):
        if pk is None:
            pk = self.pks[0]

        pk = format_column(pk)
        if limit is None and cursor is None:
            return [f"SELECT {pk} FROM {self.sql_name} ORDER BY {pk} DESC LIMIT 1"]

        where = ""
        if cursor:
            where = f"\n  WHERE {pk} > $1\n"
        if limit:
            limit = f"\n  LIMIT {int(limit)}\n"
        query = [
            f"SELECT T.{pk}\n"
            f"FROM (\n"
            f"  SELECT {pk}\n"
            f"  FROM {self.sql_name}{where}\n"
            f"  ORDER BY {pk}{limit}"
            f") AS T ORDER BY T.{pk} DESC LIMIT 1"
        ]
        if cursor:
            query.append(cursor)
        return query

    @cached_property
    def full_name(self):
        return f'{self.namespace.name}.{self.name}'

    @cached_property
    def sql_name(self):
        return format_table(self.name, schema=self.namespace.name)

    def can_order(self, column_name):
        column = self.columns[column_name]
        return can_order(column["type"])

    def is_boolean(self, column_name):
        column = self.columns[column_name]
        type = column["type"]
        return type == "boolean"

    def is_array(self, name):
        column = self.columns[name]
        type = column["type"]
        return "[]" in type or "idvector" in type or "intvector" in type

    def is_short(self, name):
        column = self.columns[name]
        return column["type"] != "pg_node_tree"

    def is_uuid(self, name):
        column = self.columns[name]
        return column["type"] != "uuid"

    def get_count_query(self):
        return (f'SELECT COUNT(*) FROM "{self.namespace.name}"."{self.name}"',)

    async def get_data_range_query(self, keys):
        new_keys = []
        for key in keys:
            new_keys.append(
                f'MIN({format_column(key)}) AS "min_{key}", '
                f'MAX({format_column(key)}) AS "max_{key}"'
            )
        keys = ",\n  ".join(new_keys)
        return (f"SELECT {keys}\n" f'FROM "{self.namespace.name}"."{self.name}"',)

    async def get_data_range(self):
        keys = copy(self.pks) if len(self.pks) == 1 else []
        keys = [key for key in keys if self.can_order(key)]
        if self.on_create:
            keys.append(self.on_create)
        if self.on_update:
            keys.append(self.on_update)

        if not keys:
            return None

        keys = list(set(keys))
        query = await self.get_data_range_query(keys)
        try:
            row = await self.database.query_one_row(*query)
        except Exception as e:
            # some columns cannot be min/maxd
            # in this case, try to use ORDER BY,
            # which works anyway for UUID
            e_ = str(e).lower()
            if not ('max' in e_ or 'min' in e_):
                raise
            tasks = []
            names = []
            for key in keys:
                names.append(f'min_{key}')
                names.append(f'max_{key}')
                tasks.append(self.get_min_id(pk=key))
                tasks.append(self.get_max_id(pk=key))

            results = await asyncio.gather(*tasks)
            result = defaultdict(dict)
            for key, r in zip(names, results):
                type = key[0:3]
                key = key[4:]
                result[key][type] = r
            return result
        else:
            result = defaultdict(dict)
            for key, value in row.items():
                type = key[0:3]
                key = key[4:]
                result[key][type] = value
            return result

    async def get_min_id(self, limit=None, cursor=None, pk=None):
        query = self.get_min_id_query(limit=limit, cursor=cursor, pk=pk)
        return await self.database.query_one_value(*query)

    async def get_max_id(self, limit=None, cursor=None, pk=None):
        query = self.get_max_id_query(limit=limit, cursor=cursor, pk=pk)
        return await self.database.query_one_value(*query)

    async def get_count(self):
        query = self.get_count_query()
        return await self.database.query_one_value(*query)

    @cached_property
    async def count(self):
        return self.get_count()
