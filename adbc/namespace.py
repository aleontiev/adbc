from cached_property import cached_property
from collections import defaultdict

import re
import json

from .exceptions import NotIncluded
from .store import ParentStore, WithConfig
from .utils import get_include_query
from .table import Table


INDEX_COLUMNS_REGEX = re.compile('.*USING [a-z_]+ [(](["A-Za-z_, ]+)[)]$')

GET_TABLE_COLUMNS_QUERY = """
SELECT
    R.relname as name,
    A.attname as column,
    pg_catalog.format_type(A.atttypid, A.atttypmod) as type,
    pg_get_expr(D.adbin, D.adrelid) as default,
    NOT A.attnotnull AS null
FROM pg_attribute A
INNER JOIN pg_class R ON R.oid = A.attrelid
INNER JOIN pg_namespace N ON R.relnamespace = N.oid
LEFT JOIN pg_attrdef D ON A.atthasdef = true AND D.adrelid = R.oid AND
    D.adnum = A.attnum
WHERE N.nspname = '{namespace}' AND A.attnum > 0 AND R.relkind = 'r'
 AND NOT A.attisdropped {query}
"""


GET_TABLE_CONSTRAINTS_QUERY = """SELECT
    R.relname as name,
    C.conname as constraint,
    C.condeferrable as deferrable,
    C.condeferred as deferred,
    C.contype::varchar as type,
    F.relname as related_name,
    C.consrc as check,
    Rel.attname as related_columns,
    A.attname as columns
FROM pg_class R
JOIN pg_constraint C ON C.conrelid = R.oid
JOIN pg_namespace N ON N.oid = R.relnamespace
LEFT JOIN pg_class F ON F.oid = C.confrelid
LEFT JOIN pg_attribute Rel ON F.oid = Rel.attrelid AND Rel.attnum = ANY(C.confkey)
LEFT JOIN pg_attribute A ON R.oid = A.attrelid AND A.attnum = ANY(C.conkey)
WHERE N.nspname = '{namespace}' and R.relkind = 'r' {query}
"""


GET_TABLE_INDEXES_QUERY = """SELECT
    R.relname as name,
    IR.relname as index,
    IA.amname as type,
    I.indisprimary as primary,
    I.indisunique as unique,
    pg_get_indexdef(I.indexrelid) as def
FROM pg_class R
JOIN pg_index I ON R.oid = I.indrelid
JOIN pg_class IR ON IR.oid = I.indexrelid
JOIN pg_namespace N ON N.oid = R.relnamespace
LEFT JOIN pg_am IA ON IA.oid = IR.relam
WHERE N.nspname = '{namespace}' and R.relkind = 'r' {query}
"""

GET_TABLES_QUERY = """SELECT
    Columns.name,
    Columns.result as columns,
    Constraints.result as constraints,
    Indexes.result as indexes
FROM (
    SELECT
        R.relname as name,
        json_agg(json_build_object(
            'name', A.attname,
            'type', pg_catalog.format_type(A.atttypid, A.atttypmod),
            'default', pg_get_expr(D.adbin, D.adrelid),
            'null', NOT A.attnotnull
        )) as result
    FROM pg_attribute A
    INNER JOIN pg_class R ON R.oid = A.attrelid
    INNER JOIN pg_namespace N ON R.relnamespace = N.oid
    LEFT JOIN pg_attrdef D ON A.atthasdef = true AND D.adrelid = R.oid AND
        D.adnum = A.attnum
    WHERE A.attnum > 0 AND N.nspname = '{namespace}' AND R.relkind = 'r'
          {query} AND NOT A.attisdropped
    GROUP BY R.relname
) Columns
LEFT JOIN (
    SELECT
        R.relname as name,
        json_agg(json_build_object(
            'name', C.conname,
            'deferrable', C.condeferrable,
            'deferred', C.condeferred,
            'type', C.contype,
            'related_columns', array_to_json(array(
                SELECT attname FROM pg_attribute
                WHERE attnum = ANY(C.confkey) AND attrelid = F.oid
            )),
            'columns', array_to_json(array(
                SELECT attname FROM pg_attribute
                WHERE attnum = ANY(C.conkey) AND attrelid = R.oid
            )),
            'related_name', F.relname,
            'check', C.consrc
        )) as result
    FROM pg_class R
    JOIN pg_constraint C ON C.conrelid = R.oid
    JOIN pg_namespace N ON N.oid = R.relnamespace
    LEFT JOIN pg_class I ON C.conindid = I.oid
    LEFT JOIN pg_class F ON F.oid = C.confrelid
    WHERE N.nspname = '{namespace}' and R.relkind = 'r' {query}
    GROUP BY R.relname
) Constraints ON Columns.name = Constraints.name
LEFT JOIN (
    SELECT
        R.relname as name,
        json_agg(json_build_object(
            'name', IR.relname,
            'type', IA.amname,
            'primary', I.indisprimary,
            'unique', I.indisunique,
            'columns', array_to_json(array(
                SELECT attname FROM pg_attribute
                WHERE attnum = ANY(I.indkey) AND attrelid = R.oid
            ))
        )) as result
    FROM pg_class R
    JOIN pg_index I ON R.oid = I.indrelid
    JOIN pg_class IR ON IR.oid = I.indexrelid
    JOIN pg_namespace N ON N.oid = R.relnamespace
    LEFT JOIN pg_am IA ON IA.oid = IR.relam
    WHERE N.nspname = '{namespace}' and R.relkind = 'r' {query}
    GROUP BY R.relname
) Indexes ON Indexes.name = Columns.name;
"""


class Namespace(WithConfig, ParentStore):
    type = 'ns'
    child_key = 'tables'

    def __init__(
        self,
        name,
        database=None,
        config=None,
        verbose=False,
        tag=None,
    ):
        self.name = name
        self.parent = self.database = database
        self.config = config
        self.verbose = verbose
        self.tag = tag
        self.log(f'init: {self}')

    def __str__(self):
        return f'{self.database}.{self.name}'

    def get_tables_query(self):
        table = "R"
        column = "relname"
        args = []
        include = self.get_child_include()
        query, args = get_include_query(
            include, table, column
        )
        if query:
            query = f" AND ({query})"
        args.insert(0, GET_TABLES_QUERY.format(namespace=self.name, query=query))
        return args

    def get_table_indexes_query(self):
        table = "R"
        column = "relname"
        args = []
        include = self.get_child_include()
        query, args = get_include_query(include, table, column)
        if query:
            query = f" AND ({query})"
        args.insert(
            0,
            GET_TABLE_INDEXES_QUERY.format(
                namespace=self.name, query=query
            )
        )
        return args

    def get_table_constraints_query(self):
        table = "R"
        column = "relname"
        args = []
        include = self.get_child_include()
        query, args = get_include_query(include, table, column)
        if query:
            query = f" AND ({query})"
        args.insert(
            0,
            GET_TABLE_CONSTRAINTS_QUERY.format(
                namespace=self.name, query=query
            )
        )
        return args

    def get_table_columns_query(self):
        table = "R"
        column = "relname"
        args = []
        include = self.get_child_include()
        query, args = get_include_query(include, table, column)
        if query:
            query = f" AND ({query})"
        args.insert(
            0,
            GET_TABLE_COLUMNS_QUERY.format(
                namespace=self.name, query=query
            )
        )
        return args

    def get_table(self, name, columns, constraints, indexes):
        config = self.get_child_config(name)
        return Table(
            name,
            config=config,
            namespace=self,
            columns=columns,
            constraints=constraints,
            indexes=indexes,
            verbose=self.verbose,
            tag=self.tag
        )

    def parse_index_columns(self, definition):
        match = INDEX_COLUMNS_REGEX.match(definition)
        if match:
            return [x.strip().replace('"', '') for x in match.group(1).split(',')]
        raise Exception(f'invalid index definition: "{definition}"')

    async def get_children(self):
        version = await self.database.version
        pool = await self.database.pool
        tables = defaultdict(dict)
        async with pool.acquire() as connection:
            async with connection.transaction():
                if version < 9:
                    columns_query = self.get_table_columns_query()
                    constraints_query = self.get_table_constraints_query()
                    indexes_query = self.get_table_indexes_query()
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
                        if 'name' not in tables[name]:
                            tables[name]['name'] = name

                        if 'columns' not in tables[name]:
                            tables[name]['columns'] = []

                        tables[name]['columns'].append({
                            'name': record[1],
                            'type': record[2],
                            'default': record[3],
                            'null': record[4]
                        })
                    for record in constraints:
                        # name, deferrable, deferred, type,
                        # related_name, check
                        # +related_columns (name), columns

                        name = record[0]
                        if 'name' not in tables[name]:
                            tables[name]['name'] = name

                        if 'constraints' not in tables[name]:
                            # constraint name -> constraint data
                            tables[name]['constraints'] = {}

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

                        cs = tables[name]['constraints']
                        if constraint not in cs:
                            cs[constraint] = {
                                'name': constraint,
                                'deferrable': record[2],
                                'deferred': record[3],
                                'type': str(record[4]),
                                'related_name': record[5],
                                'check': record[6],
                                'related_columns': related_columns,
                                'columns': attrs
                            }
                        else:
                            if related_columns:
                                constraints[name]['related_columns'].extend(
                                    related_columns
                                )
                            if attrs:
                                constraints[name]['columns'].extend(
                                    attrs
                                )

                    for record in indexes:
                        # name, type, primary, unique, def
                        name = record[0]

                        if 'name' not in tables[name]:
                            tables[name]['name'] = name

                        if 'indexes' not in tables[name]:
                            tables[name]['indexes'] = {}

                        index = record[1]
                        inds = tables[name]['indexes']
                        columns = self.parse_index_columns(record[5])
                        if index not in inds:
                            inds[index] = {
                                'name': index,
                                'type': record[2],
                                'primary': record[3],
                                'unique': record[4],
                                'columns': columns
                            }
                else:
                    query = self.get_tables_query()
                    await connection.set_type_codec(
                        "json",
                        encoder=json.dumps,
                        decoder=json.loads,
                        schema="pg_catalog"
                    )
                    async for row in connection.cursor(*query):
                        try:
                            table = self.get_table(
                                row[0],
                                row[1],
                                row[2],
                                row[3]
                            )
                        except NotIncluded:
                            pass
                        else:
                            yield table

        for table in tables.values():
            try:
                yield self.get_table(
                    table['name'],
                    table.get('columns', []),
                    list(table.get('constraints', {}).values()),
                    list(table.get('indexes', {}).values())
                )
            except NotIncluded:
                pass

    @cached_property
    async def tables(self):
        return await self.get_children()
