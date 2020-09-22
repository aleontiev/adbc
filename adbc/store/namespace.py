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
        return await self.database.backend.get_tables(self, scope)

    @cached_property
    async def tables(self):
        tables = {}
        children = await self.get_children()
        for child in children:
            tables[child.name] = child
        return tables
