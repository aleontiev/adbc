import asyncio
from copy import copy
from collections import defaultdict
from adbc.logging import Loggable
from adbc.sql import get_pks, can_order
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
        alias=None,
        **kwargs,
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
        self.alias = alias or name
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
                if "default" in column:
                    default = column["default"]
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

    async def get_info(self, schema=True, data=True, hashes=False, **kwargs):
        result = {}
        if data:
            data_range = self.get_data_range()
            count = self.get_count()
            jobs = [data_range, count]
            data_hashes = None
            if hashes:
                data_hashes = self.get_data_hashes()
                jobs.append(data_hashes)

            results = await asyncio.gather(*jobs)
            if hashes:
                data_range, count, data_hashes = results
            else:
                data_range, count = results

            result["data"] = {
                "count": count,
                "range": data_range,
            }
            if hashes:
                result["data"]["hashes"] = data_hashes
        if schema:
            result["schema"] = self.get_schema()

        self.log(f"{self}: info")
        return result

    def get_schema(self):
        result = {"columns": self.columns}
        if self.constraints is not None:
            result["constraints"] = self.constraints
        if self.indexes is not None:
            result["indexes"] = self.indexes
        return result

    def get_decode_boolean(self, column):
        F = self.database.F
        column = self.F.column(column)
        return f"decode({column}, true, 'true', false, 'false') as {column}"

    async def get_data_hashes(self, shard_size=None):
        if shard_size is None:
            shard_size = await self.database.shard_size

        cursor = None
        hashes = {}
        while True:
            stats = await self.get_statistics(
                cursor=cursor,
                limit=shard_size,
                count=True,
                max_pk=True,
                min_pk=True,
                md5=True,
            )
            min_pk = stats["min"]
            max_pk = stats["max"]
            count = stats["count"]
            md5 = stats["md5"]
            if count and md5:
                hashes[min_pk] = md5
            cursor = max_pk
            if count < shard_size:
                break

        return hashes

    async def get_statistics(
        self,
        count=False,
        min_pk=False,
        max_pk=False,
        md5=False,
        limit=None,
        cursor=None,
    ):
        split = False
        if (min_pk or max_pk) and (md5 or count):
            # may need to split up this query
            # if the pk is a UUID then min_pk and max_pk have to run separate
            if self.pks:
                pk = self.pks[0]
                split = self.columns[pk]["type"] == "uuid"
        if split:
            # split query:
            # call this function several times with reduced parameter set
            kwargs["min_pk"] = False
            kwargs["max_pk"] = False

            if md5 or count:
                md5 = self.get_statistics(**kwargs)
            else:
                md5 = False
            if min_pk:
                min_pk = self.get_min_id(cursor=cursor, limit=limit)
            if max_pk:
                max_pk = self.get_max_id(cursor=cursor, limit=limit)

            tasks = []
            if md5:
                tasks.append(md5)
            if min_pk:
                tasks.append(min_pk)
            if max_pk:
                tasks.append(max_pk)

            results = await gather(*tasks)
            i = 0

            if md5:
                md5 = results[i]
                i += 1
            if min_pk:
                min_pk = results[i]
                i += 1

            if max_pk:
                max_pk = results[i]
                i += 1

            result = {}
            if md5:
                result.update(md5.items())
            if min_pk:
                result["min"] = min_pk
            if max_pk:
                result["max"] = max_pk
            return result

        else:
            query = await self.get_statistics_query(
                max_pk=max_pk,
                limit=limit,
                cursor=cursor,
                min_pk=min_pk,
                count=count,
                md5=md5,
            )
            result = await self.database.query_one_row(*query)
            return result

    async def get_statistics_query(
        self,
        count=False,
        max_pk=False,
        min_pk=False,
        md5=False,
        limit=None,
        cursor=None,
    ):
        # TODO: refactor to JSQL
        if not count and not max_pk and not md5 and not min_pk:
            raise Exception("must pass count or max_pk or md5 or min_pk")

        F = self.database.F
        redshift = await self.database.is_redshift
        decode = False
        aggregator = "array_to_string(array_agg"
        end = "), ';')"
        cast = False
        if redshift:
            # TODO: fix, technically this applies to Redshift, not Postgres <9
            # in practice, nobody else is running Postgres 8 anymore...
            aggregator = "listagg"
            end = ", ';')"
            decode = True
            cast = True

        columns = list(sorted(self.columns.keys()))
        pks = self.pks
        order = ", ".join([F.column(c) for c in pks if self.can_order(c)])
        cast = "::varchar" if cast else ""

        if not md5:
            columns = pks
        columns_ = [F.column(c) for c in columns]

        # concatenate all column names and values in pseudo-json
        aggregate = ",".join(
            [f"T.{c}{cast}" for c in columns_]
        )
        aggregate = f"concat_ws(',', {aggregate})"

        inner = ", ".join(
            [
                self.get_decode_boolean(c)
                if decode and self.is_boolean(c)
                else (
                    columns_[i]
                    if not self.is_array(c)
                    else f"concat('[', array_to_string({columns_[i]}, ','), ']') {columns_[i]}"
                )
                for i, c in enumerate(columns)
            ]
        )
        output = []
        pk = pks[0]
        md5 = f"md5({aggregator}({aggregate}{end}) as md5" if md5 else None
        count = "count(*)" if count else ""
        pk_ = F.column(pk)
        max_pk = f"max({pk_})" if max_pk else ""
        min_pk = f"min({pk_})" if min_pk else ""
        if md5:
            output.append(md5)
        if count:
            output.append(count)
        if max_pk:
            output.append(max_pk)
        if min_pk:
            output.append(min_pk)

        if limit:
            limit = int(limit)
            limit = f"\n  LIMIT {limit}"
        else:
            limit = ""

        where = ""
        if cursor:
            where = f"\n  WHERE {pk_} > $1"

        output = ", ".join(output)
        query = [
            f"SELECT {output}\n"
            f"FROM (\n"
            f"  SELECT {inner}\n"
            f"  FROM {self.sql_name}{where}\n"
            f"  ORDER BY {order}{limit}\n"
            f") T"
        ]
        if cursor:
            query.append(cursor)
        return query

    def get_min_id_query(self, limit=None, cursor=None, pk=None):
        if pk is None:
            pk = self.pks[0]

        pk = self.database.F.column(pk)
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
            f") T LIMIT 1"
        ]
        if cursor:
            query.append(cursor)
        return query

    def get_max_id_query(self, limit=None, cursor=None, pk=None):
        if pk is None:
            pk = self.pks[0]

        F = self.database.F
        pk = F.column(pk)
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
            f") T\n"
            f"ORDER BY T.{pk} DESC\n"
            "LIMIT 1"
        ]
        if cursor:
            query.append(cursor)
        return query

    @cached_property
    def full_name(self):
        return f"{self.namespace.name}.{self.name}"

    @cached_property
    def sql_name(self):
        return self.database.F.table(self.name, schema=self.namespace.name)

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
        # TODO: move into Q
        return (f'SELECT count(*) FROM {self.sql_name}', )

    def get_data_range_query(self, keys):
        new_keys = []
        F = self.database.F
        for key in keys:
            column = F.column(key)
            min_key = F.column(f'min_{key}')
            max_key = F.column(f'max_{key}')
            new_keys.append(
                f'min({column}) AS {min_key}, '
                f'max({column}) AS {max_key}'
            )
        keys = ",\n  ".join(new_keys)
        return (f"SELECT {keys}\n" f'FROM {self.sql_name}', )

    async def get_data_range(self, keys=None):
        if keys is None:
            keys = copy(self.pks) if len(self.pks) == 1 else []
            keys = [key for key in keys if self.can_order(key)]
            if self.on_create:
                keys.append(self.on_create)
            if self.on_update:
                keys.append(self.on_update)
            keys = list(set(keys))

        if not keys:
            return None

        query = self.get_data_range_query(keys)
        try:
            row = await self.database.query_one_row(*query)
        except Exception as e:
            # some columns cannot be min/maxd
            # in this case, try to use ORDER BY,
            # which works anyway for UUID
            e_ = str(e).lower()
            if not ("max" in e_ or "min" in e_):
                raise
            tasks = []
            names = []
            for key in keys:
                names.append(f"min_{key}")
                names.append(f"max_{key}")
                tasks.append(self.get_min_id(pk=key))
                tasks.append(self.get_max_id(pk=key))

            results = await asyncio.gather(*tasks)
            result = defaultdict(dict)
            for key, r in zip(names, results):
                type = key[0:3]
                key = key[4:]
                result[key][type] = r
            return dict(result)
        else:
            result = defaultdict(dict)
            for key, value in row.items():
                type = key[0:3]
                key = key[4:]
                result[key][type] = value
            return dict(result)

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
