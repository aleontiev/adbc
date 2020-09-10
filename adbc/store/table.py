import asyncio
import re

from typing import Union
from copy import copy
from collections import defaultdict
from adbc.exceptions import NotIncluded
from adbc.sql import can_order
from adbc.logging import Loggable
from adbc.scope import WithScope
from adbc.constants import SEQUENCE, TABLE, PRIMARY, UNIQUE, FOREIGN
from cached_property import cached_property
from adbc.utils import get_first



def get_fks(constraints):
    """Get foreign key fields given constraint list"""
    fks = {}

    if constraints:
        for name, constraint in constraints.items():
            if constraint['type'] == FOREIGN and len(constraint['columns']) == 1:
                column = constraint['columns'][0]
                fks[column] = {
                    'to': constraint['related_name'],
                    'by': constraint['related_columns'],
                    'name': name
                }

    return fks


def get_pks(constraints):
    """Get primary key(s) given constraint list"""
    pks = {}

    if constraints:
        for name, constraint in constraints.items():
            if constraint['type'] == PRIMARY and len(constraint['columns']) == 1:
                column = constraint['columns'][0]
                pks[column] = name

    return pks


def get_uniques(constraints):
    """Get unique key(s) given constraint list"""
    uniques = {}
    if constraints:
        for name, constraint in constraints.items():
            if constraint["type"] == UNIQUE and len(constraint['columns']) == 1:
                column = constraint['columns'][0]
                uniques[column] = name
    return uniques


class Table(WithScope, Loggable):
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
        type=None,
        **kwargs,
    ):
        super().__init__(**kwargs)
        if not isinstance(scope, dict):
            self.scope = {}
        else:
            self.scope = scope

        self.type = type or TABLE
        self.name = name
        self.verbose = verbose
        self.parent = self.namespace = namespace
        self.database = namespace.database
        self.alias = alias or name
        self.tag = tag

        self.on_create = self.scope.get("on_create", None)
        self.on_update = self.scope.get("on_update", None)
        self.on_delete = self.scope.get("on_update", None)
        self.immutable = self.scope.get(
            "immutable", not (bool(self.on_update) or bool(self.on_delete))
        )

        self.columns = self.get_children("columns", columns)
        self.column_names = list(self.columns.keys())
        self.constraints = self.get_children("constraints", constraints or [])
        self.indexes = self.get_children("indexes", indexes or [])

        if self.type == SEQUENCE:
            # sequences do not have constraints/indexes
            self.init_sequence()
        else:
            self.init_table()

    def init_sequence(self):
        pass

    def init_table(self):
        """Normal table initializer"""
        # in one loop, create a two-way values binding for these properties:
        # - sequence: based on auto-increment (MySQL) or nextval (Postgres)
        # - primary: based on primary constraint or index
        # - unique: based on unique constraints
        # - related: based on foreign key constraints
        constraints = self.constraints
        pks = self.pks = get_pks(constraints) or self.column_names
        uniques = self.uniques = get_uniques(constraints)
        fks = self.fks = get_fks(constraints)

        for name, column in self.columns.items():
            if "default" in column:
                default = column["default"]
                default = column['default'] = self.database.backend.parse_expression(
                    default
                )

                if isinstance(default, dict) and 'nextval' in default:
                    # TODO: move nextval into backend, non-standard SQL
                    column["sequence"] = column.get(
                        "sequence", default['nextval'][1:-1]
                    )
                else:
                    column["sequence"] = column.get("sequence", False)

            if column.get("primary"):
                if name not in pks:
                    primary = column.get("primary")
                    constraint_name = (
                        primary
                        if isinstance(primary, str)
                        else f"{self.name}__{name}__pk"
                    )
                    pks[name] = constraint_name
                    constraints[constraint_name] = {
                        "type": PRIMARY,
                        "columns": [name],
                    }
            else:
                column["primary"] = pks.get(name, False)
            if column.get("unique"):
                if name not in uniques:
                    unique = column.get("unique")
                    constraint_name = (
                        unique if isinstance(unique, str) else f"{self.name}__{name}__uk"
                    )
                    uniques[name] = constraint_name
                    constraints[constraint_name] = {
                        "type": UNIQUE,
                        "columns": [name],
                    }
            else:
                column["unique"] = uniques.get(name, False)

            related = column.get('related')
            if related:
                if name not in fks:
                    constraint_name = (
                        related if isinstance(related, str) else f"{self.name}__{name}__fk"
                    )
                    by = related['by']
                    to = related['to'],
                    if not isinstance(by, list):
                        by = [by]

                    fks[name] = {
                        'to': related['to'],
                        'by': by,
                        'name': constraint_name
                    }
                    constraints[constraint_name] = {
                        "type": FOREIGN,
                        "columns": [name],
                        "related_name": to,
                        "related_columns": by
                    }
            else:
                column['related'] = fks.get(name, None)

    def __str__(self):
        return f"{self.namespace}.{self.name}"

    def get_children(self, child_key: str, children: list):
        result = {}
        translation = self.get_scope_translation(
            self.scope, from_=self.tag, child_key=child_key
        )
        translate = lambda x: translation.get(x, x)
        scope = self.scope
        for child in sorted(children, key=lambda c: translate(c["name"])):
            # real schema name
            name = child.pop("name")
            # alias name
            alias = translate(name)
            if alias != name:
                child["alias"] = alias
            try:
                child_scope = self.get_child_scope(
                    name, scope=scope, child_key=child_key
                )
            except NotIncluded:
                continue
            else:
                result[name] = child
        return result

    async def get_sequence_last_value(self):
        query = {
            'select': {
                'data': 'last_value',
                'from': self.full_name,
            }
        }
        return await self.database.query_one_value(query)

    async def get_info(self, schema=True, data=True, hashes=False, **kwargs):
        result = {}
        exclude = kwargs.get("exclude", None)
        if data:
            if self.type == 'table':
                data_range = self.get_data_range()
                count = self.get_count()
                jobs = [data_range, count]
                data_hashes = None
                if hashes:
                    shard_size = (
                        hashes if hashes is not True and isinstance(hashes, int)
                        else None
                    )
                    data_hashes = self.get_data_hashes(shard_size=shard_size)
                    jobs.append(data_hashes)

                results = await asyncio.gather(*jobs)
                if hashes:
                    data_range, count, data_hashes = results
                else:
                    data_range, count = results

                result["rows"] = {
                    "count": count,
                    "range": data_range,
                }
                if hashes:
                    result["rows"]["hashes"] = data_hashes
            else: # sequence
                value = await self.get_sequence_last_value()
                result['value'] = value

        schema = self.get_schema(exclude=exclude)
        result.update(schema)

        result["type"] = self.type

        self.log(f"{self}: info")
        return result

    def realias(self, result: dict):
        new_result = {}
        for key, value in result.items():
            if 'alias' in value:
                value = copy(value)
                alias = value.pop('alias')
            else:
                alias = key
            new_result[alias] = value
        return new_result

    def get_schema(self, exclude=None):
        """
        exclude:
            e.g: {
                "columns": ['default'],
                "constraints": ['deferrable', 'deferred']
            }
        """
        if self.type == 'table':
            exclude = exclude or {}
            result = {
                "columns": self.realias(
                    self.exclude(self.columns, exclude.get("columns"))
                )
            }
            if self.constraints is not None:
                result["constraints"] = self.realias(
                    self.exclude(
                        self.constraints, exclude.get("constraints")
                    )
                )

            if self.indexes is not None:
                result["indexes"] = self.realias(self.exclude(self.indexes, exclude.get("indexes")))
            return result
        else:
            # TODO: schema for sequences: min, max, start, increment_by
            return {}

    def exclude(self, source: dict, exclude: Union[list, dict]) -> dict:
        if not exclude:
            return source
        if isinstance(exclude, list):
            fields = exclude
            names = None
            types = None
        else:
            names = exclude.get("names", None)
            fields = exclude.get("fields", None)
            types = exclude.get("types", None)

        result = {}
        for key, value in source.items():
            if names and key in names:
                continue
            if types and value.get("type") in types:
                continue
            if fields:
                for f in fields:
                    value.pop(f, None)

            result[key] = value
        return result

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
            # if the pk is a UUID then min_pk and max_pk have to run separately
            if self.pks:
                pk = next(iter(self.pks))
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
            query, params = await self.get_statistics_query(
                max_pk=max_pk,
                limit=limit,
                cursor=cursor,
                min_pk=min_pk,
                count=count,
                md5=md5,
            )
            result = await self.database.query_one_row(query, params)
            return result

    def order_by_alias(self, columns):
        return sorted(
            columns,
            key=lambda c: self.columns[c].get('alias', c)
        )

    async def get_statistics_query(
        self,
        count=False,
        max_pk=False,
        min_pk=False,
        md5=False,
        limit=None,
        cursor=None,
    ):
        # TODO: refactor to PreQL
        if not count and not max_pk and not md5 and not min_pk:
            raise Exception("must pass count or max_pk or md5 or min_pk")

        F = self.database.F
        columns = list(sorted(self.columns.keys()))
        pks = self.order_by_alias(self.pks)

        order = ", ".join([F.column(c) for c in pks if self.can_order(c)])

        if not md5:
            columns = pks

        # TODO: use alias ordering to ensure consistent
        # hashes across datastores with different schematic names
        columns_ = [F.column(c) for c in self.order_by_alias(columns)]
        # concatenate values together 
        aggregate = ",".join([f"T.{c}" for c in columns_])
        aggregate = f"json_build_array({aggregate})"

        inner = ", ".join(columns_)
        output = []
        pk = pks[0]
        md5 = f"md5(array_to_string(array_agg({aggregate}), ',')) as md5" if md5 else None
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
        query = (
            f"SELECT {output}\n"
            f"FROM (\n"
            f"  SELECT {inner}\n"
            f"  FROM {self.sql_name}{where}\n"
            f"  ORDER BY {order}{limit}\n"
            f") T"
        )
        params = []
        if cursor:
            params.append(cursor)
        return query, params

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
        return (f"SELECT count(*) FROM {self.sql_name}",)

    def get_data_range_query(self, keys):
        new_keys = []
        F = self.database.F
        for key in keys:
            column = F.column(key)
            min_key = F.column(f"min_{key}")
            max_key = F.column(f"max_{key}")
            new_keys.append(
                f"min({column}) AS {min_key}, " f"max({column}) AS {max_key}"
            )
        keys = ",\n  ".join(new_keys)
        return (f"SELECT {keys}\n" f"FROM {self.sql_name}",)

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
