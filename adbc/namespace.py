from cached_property import cached_property
from collections import defaultdict

import re

from .exceptions import NotIncluded
from .store import ParentStore, WithConfig
from .table import Table


INDEX_COLUMNS_REGEX = re.compile('.*USING [a-z_]+ [(](["A-Za-z_, ]+)[)]$')


class Namespace(WithConfig, ParentStore):
    type = "ns"
    child_key = "tables"

    def __init__(
        self, name, database=None, config=None, verbose=False, tag=None, **kwargs
    ):
        super().__init__(**kwargs)
        self.name = name
        self.parent = self.database = database
        self.config = config
        self.verbose = verbose
        self.tag = tag
        self._tables = {}

    def __str__(self):
        return f"{self.database}.{self.name}"

    def get_table(
        self, name, columns=None, constraints=None, indexes=None, refresh=False
    ):
        if name not in self._tables or refresh:
            assert columns is not None
            config = self.get_child_config(name)
            self._tables[name] = Table(
                name,
                config=config,
                namespace=self,
                columns=columns,
                constraints=constraints,
                indexes=indexes,
                verbose=self.verbose,
                tag=self.tag,
            )
        return self._tables[name]

    def parse_index_columns(self, definition):
        match = INDEX_COLUMNS_REGEX.match(definition)
        if match:
            return [x.strip().replace('"', "") for x in match.group(1).split(",")]
        raise Exception(f'invalid index definition: "{definition}"')

    def get_query(self, name):
        include = self.get_child_include()
        return self.database.backend.get_query(name, self.name, include)

    async def get_children(self, refresh=False):
        json_aggregation = self.database.backend.has("json_aggregation")
        pool = await self.database.pool
        tables = defaultdict(dict)
        async with pool.acquire() as connection:
            async with connection.transaction():
                if not json_aggregation:
                    columns_query = self.get_query("table_columns")
                    constraints_query = self.get_query("table_constraints")
                    indexes_query = self.get_query("table_indexes")
                    # tried running in parallel, but issues with Redshift
                    # columns, constraints, indexes = await asyncio.gather(
                    #    columns, constraints, indexes
                    # )
                    indexes = connection.fetch(*indexes_query)
                    indexes = await indexes

                    columns = connection.fetch(*columns_query)
                    columns = await columns

                    constraints = connection.fetch(*constraints_query)
                    constraints = await constraints

                    for record in columns:
                        # name, column, type, default, null
                        name = record[0]
                        if "name" not in tables[name]:
                            tables[name]["name"] = name

                        if "columns" not in tables[name]:
                            tables[name]["columns"] = []

                        tables[name]["columns"].append(
                            {
                                "name": record[1],
                                "type": record[2],
                                "default": record[3],
                                "null": record[4],
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
                    query = self.get_query("tables")
                    async for row in connection.cursor(*query):
                        try:
                            table = self.get_table(
                                row[0], row[1], row[2], row[3], refresh=refresh
                            )
                        except NotIncluded:
                            pass
                        else:
                            yield table

        for table in tables.values():
            try:
                yield self.get_table(
                    table["name"],
                    table.get("columns", []),
                    list(table.get("constraints", {}).values()),
                    list(table.get("indexes", {}).values()),
                    refresh=refresh,
                )
            except NotIncluded:
                pass

    @cached_property
    async def tables(self):
        tables = {}
        async for child in self.get_children():
            tables[child.name] = child
        return tables
