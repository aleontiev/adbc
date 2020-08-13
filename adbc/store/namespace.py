from cached_property import cached_property
from collections import defaultdict

import re

from adbc.logging import Loggable
from adbc.exceptions import NotIncluded
from adbc.scope import WithScope

from adbc.operations.info import WithInfo

from .table import Table


INDEX_COLUMNS_REGEX = re.compile('.*USING [a-z_]+ [(](["A-Za-z_, ]+)[)]$')


class Namespace(Loggable, WithScope, WithInfo):
    child_key = "tables"

    def __init__(
        self,
        name,
        database=None,
        scope=None,
        verbose=False,
        tag=None,
        alias=None,
        **kwargs,
    ):
        super().__init__(**kwargs)
        self.name = name
        self.parent = self.database = database
        self.scope = scope
        self.alias = alias or name
        self.verbose = verbose
        self.tag = tag

    def __str__(self):
        return f"{self.database}.{self.name}"

    def get_table(
        self,
        name,
        columns=None,
        constraints=None,
        indexes=None,
        scope=None,
        type=None,
    ):
        scope = scope or self.scope
        return self.cache_by(
            'tables',
            {'scope': scope, 'name': name},
            lambda: self._get_table(name, columns, constraints, indexes, scope, type)
        )

    def _get_table(
        self,
        name,
        columns,
        constraints,
        indexes,
        scope,
        type
    ):
        assert columns is not None
        translation = self.get_scope_translation(scope=scope, from_=self.tag)
        alias = translation.get(name, name)
        table_scope = self.get_child_scope(name, scope=scope)
        return Table(
            name,
            alias=alias,
            namespace=self,
            columns=columns,
            constraints=constraints,
            indexes=indexes,
            verbose=self.verbose,
            scope=table_scope,
            tag=self.tag,
            type=type
        )

    def parse_index_columns(self, definition):
        match = INDEX_COLUMNS_REGEX.match(definition)
        if match:
            return [x.strip().replace('"', "") for x in match.group(1).split(",")]
        raise Exception(f'invalid index definition: "{definition}"')

    def get_query(self, name, scope=None):
        include = self.get_child_include(scope=scope)
        return self.database.backend.get_query(name, self.name, include, tag=self.tag)

    async def get_children(self, scope=None):
        scope = scope or self.scope
        return await self.cache_by_async(
            'children',
            scope,
            lambda: self._get_children(scope)
        )

    async def _get_children(self, scope):
        json_aggregation = self.database.backend.has("json_aggregation")
        tables = defaultdict(dict)
        database = self.database
        results = []
        if not json_aggregation:
            columns_query = self.get_query("table_columns", scope=scope)
            constraints_query = self.get_query("table_constraints", scope=scope)
            indexes_query = self.get_query("table_indexes", scope=scope)

            # tried running in parallel, but issues with Redshift
            # columns, constraints, indexes = await asyncio.gather(
            #    columns, constraints, indexes
            # )

            indexes = database.query(*indexes_query)
            indexes = await indexes

            columns = database.query(*columns_query)
            columns = await columns

            constraints = database.query(*constraints_query)
            constraints = await constraints

            for record in columns:
                # name, kind, column, type, default, null
                name = record[0]
                kind = record[1]

                if "type" not in tables[name]:
                    tables[name]["type"] = kind
                if "name" not in tables[name]:
                    tables[name]["name"] = name

                if "columns" not in tables[name]:
                    tables[name]["columns"] = []

                tables[name]["columns"].append(
                    {
                        "name": record[2],
                        "type": record[3],
                        "default": record[4],
                        "null": record[5],
                    }
                )
            for record in constraints:
                # name, deferrable, deferred, type,
                # related_name, check
                # +related_columns (name), columns

                name = record[0]
                if "name" not in tables[name]:
                    tables[name]["name"] = name

                if "constraints" not in tables[name]:
                    # constraint name -> constraint data
                    tables[name]["constraints"] = {}

                constraint = record[1]
                related_columns = record[7]
                if not related_columns:
                    related_columns = []
                else:
                    related_columns = [related_columns]

                attrs = record[8]
                if not attrs:
                    attrs = []
                else:
                    attrs = [attrs]

                cs = tables[name]["constraints"]
                if constraint not in cs:
                    cs[constraint] = {
                        "name": constraint,
                        "deferrable": record[2],
                        "deferred": record[3],
                        "type": str(record[4]),
                        "related_name": record[5],
                        "check": record[6],
                        "related_columns": related_columns,
                        "columns": attrs,
                    }
                else:
                    if related_columns:
                        constraints[name]["related_columns"].extend(
                            related_columns
                        )
                    if attrs:
                        constraints[name]["columns"].extend(attrs)

            for record in indexes:
                # name, type, primary, unique, def
                name = record[0]

                if "name" not in tables[name]:
                    tables[name]["name"] = name

                if "indexes" not in tables[name]:
                    tables[name]["indexes"] = {}

                index = record[1]
                inds = tables[name]["indexes"]
                columns = self.parse_index_columns(record[5])
                if index not in inds:
                    inds[index] = {
                        "name": index,
                        "type": record[2],
                        "primary": record[3],
                        "unique": record[4],
                        "columns": columns,
                    }
        else:
            query = self.get_query("tables", scope=scope)
            async for row in database.stream(query):
                try:
                    table = self.get_table(
                        row[0],
                        columns=row[1],
                        constraints=row[2],
                        indexes=row[3],
                        type=row[4],
                        scope=scope
                    )
                except NotIncluded:
                    pass
                else:
                    results.append(table)

        for table in tables.values():
            try:
                table = self.get_table(
                    table["name"],
                    columns=table.get("columns", []),
                    constraints=list(table.get("constraints", {}).values()),
                    indexes=list(table.get("indexes", {}).values()),
                    type=table['type'],
                    scope=scope
                )
            except NotIncluded:
                pass
            else:
                results.append(table)
        return results

    @cached_property
    async def tables(self):
        tables = {}
        children = await self.get_children()
        for child in children:
            tables[child.name] = child
        return tables
