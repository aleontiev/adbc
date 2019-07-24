from cached_property import cached_property
import json

from .store import ParentStore
from .query import build_include_exclude
from .table import Table


GET_TABLES_QUERY = """SELECT
    Attributes.name,
    Attributes.result as attributes,
    Constraints.result as constraints,
    Indexes.result as indexes
FROM (
    SELECT
        R.relname as name,
        json_agg(json_build_object(
            'name', A.attname,
            'type', pg_catalog.format_type(A.atttypid, A.atttypmod),
            'default', CASE WHEN D.adsrc LIKE 'nextval%' THEN NULL ELSE D.adsrc END,
            'nullable', NOT A.attnotnull
        )) as result
    FROM pg_attribute A
    INNER JOIN pg_class R ON R.oid = A.attrelid
    INNER JOIN pg_namespace N ON R.relnamespace = N.oid
    LEFT JOIN pg_attrdef D ON A.atthasdef = true AND D.adrelid = R.oid AND
        D.adnum = A.attnum
    WHERE N.nspname = '{namespace}' and A.attnum > 0 and R.relkind = 'r' {query} and NOT A.attisdropped
    GROUP BY R.relname
) Attributes
LEFT JOIN (
    SELECT
        R.relname as name,
        json_agg(json_build_object(
            'name', C.conname,
            'deferrable', C.condeferrable,
            'deferred', C.condeferred,
            'index_name', I.relname,
            'type', C.contype,
            'related_attributes', array_to_json(array(
                SELECT attname FROM pg_attribute
                WHERE attnum = ANY(C.confkey) AND attrelid = F.oid
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
) Constraints ON Attributes.name = Constraints.name
LEFT JOIN (
    SELECT
        R.relname as name,
        json_agg(json_build_object(
            'name', IR.relname,
            'type', IA.amname,
            'keys', array_to_json(array(
                SELECT attname FROM pg_attribute
                WHERE attnum = ANY(I.indkey) AND attrelid = R.oid
            )),
            'operators', array_to_json(array(
                SELECT pg_opclass.opcname FROM pg_opclass
                WHERE pg_opclass.oid = ANY(I.indclass)
            ))
        )) as result
    FROM pg_class R
    JOIN pg_index I ON R.oid = I.indrelid
    JOIN pg_class IR ON IR.oid = I.indexrelid
    JOIN pg_namespace N ON N.oid = R.relnamespace
    LEFT JOIN pg_am IA ON IA.oid = IR.relam
    WHERE N.nspname = '{namespace}' and R.relkind = 'r' {query}
    GROUP BY R.relname
) Indexes ON Indexes.name = Attributes.name;
"""


class Namespace(ParentStore):
    def __init__(
        self,
        name,
        database=None,
        only_tables=None,
        exclude_tables=None,
        verbose=False,
        tag=None,
    ):
        self.name = name
        self.database = database
        self.only_tables = only_tables
        self.exclude_tables = exclude_tables
        self.verbose = verbose
        self.tag = tag

    async def get_children(self):
        tables = await self.tables
        return tables

    def get_tables_query(self):
        table = "R"
        column = "relname"
        args = []
        query, args = build_include_exclude(
            table, column, self.only_tables, self.exclude_tables
        )
        if query:
            query = " AND {}".format(query)
        args.insert(0, GET_TABLES_QUERY.format(namespace=self.name, query=query))
        return args

    async def get_tables(self):
        pool = await self.database.pool
        query = self.get_tables_query()
        tables = []
        async with pool.acquire() as connection:
            await connection.set_type_codec(
                "json", encoder=json.dumps, decoder=json.loads, schema="pg_catalog"
            )
            for row in await connection.fetch(*query):
                tables.append(self.get_table(row[0], row[1], row[2], row[3]))
            self.print("<- {}.namespace.{}.tables = {}".format(
                self.tag or '',
                self.name, len(tables)))
        return tables

    def get_table(self, name, attributes, constraints, indexes):
        return Table(
            name,
            namespace=self,
            attributes=attributes,
            constraints=constraints,
            indexes=indexes,
            verbose=self.verbose,
            tag=self.tag
        )

    @cached_property
    async def tables(self):
        return await self.get_tables()
