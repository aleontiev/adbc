from .base import DatabaseBackend
from cached_property import cached_property
from asyncpg import create_pool, connect
import json


VERSION_QUERY = "SELECT version()"

TABLE_COLUMNS_QUERY = """
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


TABLE_CONSTRAINTS_QUERY = """SELECT
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


TABLE_INDEXES_QUERY = """SELECT
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

"""
TABLES_JSQL = {
    "select": {
        "name": "Columns.name",
        "columns": "Columns.result",
        "constraints": "Constraints.result",
        "indexes": "Indexes.result"
    },
    "from": {
        "select": {
            "name": "R.relname",
            "result": {
                "json_agg": {
                    "json_build_object": [
                        "'name'",
                        "A.attname",
                        "'type'", {
                            "pg_catalog.format_type": [
                                "A.attypid",
                                "A.atttypmod"
                            ]
                        }
                    ]
                }
            }
        },
        "from": {
            "A": "pg_attribute",
        },
        "join": {
            "R": {
                "from": "pg_class",
                "on": {
                    "=": [
                        "R.oid",
                        "A.attrelid"
                    ]
                }
            },
            "N": {
                "from": "pg_namespace",
                "on": {
                    "=": [
                        "R.relnamespace",
                        "N.oid"
                    ]
                }
            },
            "D": {
                "type": "left",
                "from": "pg_attrdef",
                "on": {
                    "and": [{
                        "true": "A.atthasdef"
                    }, {
                        "=": [
                            "D.adrelid",
                            "R.oid"
                        ]
                    }, {
                        "=": [
                            "D.adnum",
                            "A.attnum"
                        ]
                    }]
                }
            }
        }
    },
    "join": {
        "Constraints": {
            "type": "left",
            "from": {
                ...
            }
        },
        "Indexes": {
            "type": "left",
            "on": {
                "=": [
                    "Indexes.name",
                    "Columns.name"
                ]
            }
        }
    }
}
"""

TABLES_QUERY = """SELECT
    Columns.name as name,
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
                JOIN (SELECT a, b FROM (SELECT unnest(C.confkey) as a, generate_series(1, array_length(C.confkey, 1)) as b) x) ORD on ORD.a = attnum
                WHERE attnum = ANY(C.confkey) AND attrelid = F.oid
            )),
            'columns', array_to_json(array(
                SELECT attname FROM pg_attribute
                JOIN (SELECT a, b FROM (SELECT unnest(C.conkey) as a, generate_series(1, array_length(C.conkey, 1)) as b) x) ORD on ORD.a = attnum
                WHERE attnum = ANY(C.conkey) AND attrelid = R.oid
                ORDER BY ORD.b
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
                JOIN (SELECT a, b FROM (SELECT unnest(I.indkey) as a, generate_series(1, array_length(I.indkey, 1)) as b) x) ORD on ORD.a = attnum
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
"""  # noqa


class SQLFormatter(object):
    @classmethod
    def identifier(cls, name):
        return f'"{name}"'

    @classmethod
    def column(cls, name, table=None, schema=None):
        name = cls.identifier(name)
        if table:
            table = cls.table(name, schema=schema)
            return f'{table}.{name}'
        else:
            return name

    @classmethod
    def schema(cls, name):
        return cls.identifier(name)

    @classmethod
    def constraint(cls, name, schema=None):
        return cls.table(name, schema=schema)

    @classmethod
    def index(cls, name, schema=None):
        return cls.table(name, schema=schema)

    @classmethod
    def database(cls, name):
        return cls.identifier(name)

    @classmethod
    def table(cls, name, schema=None):
        name = cls.identifier(name)
        if schema:
            schema = cls.schema(schema)
            return f'{schema}.{name}'
        return name


class PostgresSQLFormatter(SQLFormatter):
    pass


class PostgresBackend(DatabaseBackend):
    """Postgres backend based on asyncpg"""

    has_json_aggregation = True
    F = PostgresSQLFormatter()

    @staticmethod
    def get_include_clause(include, table, column, tag=None):
        """Get query filters that in/exclude based on a particular column"""

        if not include or include is True:
            # no filters
            return ("", [])

        args = []
        query = []
        count = 1
        includes = excludes = False
        for key, should in include.items():
            should_dict = isinstance(should, dict)
            if should_dict:
                should_dict = should
                if "enabled" in should:
                    should = should["enabled"]

            if not should:
                # disabled config block, skip
                continue

            should = bool(should)
            if key.startswith("~"):
                should = not should
                key = key[1:]

            wild = False
            if "*" in key:
                wild = True
                operator = "~~" if should else "!~~"
                key = key.replace("*", "%")
            else:
                operator = "=" if should else "!="

            if tag is None:
                name = key
            else:
                name = should_dict.get(tag, key) if should_dict else key
                if wild and should_dict and tag in should_dict:
                    raise ValueError(f"Cannot have tag '{name}' for wild key '{key}'")

            args.append(name)
            query.append('({}."{}" {} ${})'.format(table, column, operator, count))
            count += 1
            if should:
                includes = True
            else:
                excludes = True

        if includes and not excludes:
            union = "OR"
        else:
            union = "AND"
        result = " {} ".format(union).join(query), args
        return result

    @staticmethod
    def get_databases_query(include, tag=None):
        table = "pg_database"
        column = "datname"
        args = []
        query, args = PostgresBackend.get_include_clause(
            include, table, column, tag=tag
        )
        if query:
            query = " AND {}".format(query)

        F = PostgresBackend.F
        column = F.column(column)
        table = F.table(table)
        args.insert(
            0,
            f'SELECT {column} FROM {table} WHERE datistemplate = false {query}'
        )
        return args

    @staticmethod
    def get_namespaces_query(include, tag=None):
        table = "pg_namespace"
        column = "nspname"
        query, args = PostgresBackend.get_include_clause(
            include, table, column, tag=tag
        )
        if query:
            query = "WHERE {}".format(query)

        F = PostgresBackend.F
        column = F.column(column)
        table = F.table(table)
        args.insert(0, f'SELECT {column} FROM {table} {query}')
        return args

    @staticmethod
    def get_version_query():
        return (VERSION_QUERY,)

    @classmethod
    def get_query(cls, name, *args, **kwargs):
        return getattr(cls, f"get_{name}_query")(*args, **kwargs)

    @staticmethod
    def get_tables_query(namespace, include, tag=None):
        table = "R"
        column = "relname"
        args = []
        query, args = PostgresBackend.get_include_clause(
            include, table, column, tag=tag
        )
        if query:
            query = f" AND ({query})"
        args.insert(0, TABLES_QUERY.format(namespace=namespace, query=query))
        return args

    @staticmethod
    def get_table_indexes_query(namespace, include, tag=None):
        table = "R"
        column = "relname"
        args = []
        query, args = PostgresBackend.get_include_query(include, table, column, tag=tag)
        if query:
            query = f" AND ({query})"
        args.insert(0, TABLE_INDEXES_QUERY.format(namespace=namespace, query=query))
        return args

    @staticmethod
    def get_table_constraints_query(namespace, include, tag=None):
        table = "R"
        column = "relname"
        args = []
        query, args = PostgresBackend.get_include_clause(
            include, table, column, tag=tag
        )
        if query:
            query = f" AND ({query})"
        args.insert(0, TABLE_CONSTRAINTS_QUERY.format(namespace=namespace, query=query))
        return args

    @staticmethod
    def get_table_columns_query(namespace, include, tag=None):
        table = "R"
        column = "relname"
        args = []
        query, args = PostgresBackend.get_include_clause(
            include, table, column, tag=tag
        )
        if query:
            query = f" AND ({query})"
        args.insert(0, TABLE_COLUMNS_QUERY.format(namespace=namespace, query=query))
        return args

    @staticmethod
    async def create_pool(*args, **kwargs):
        if 'init' not in kwargs:
            # initialize connection with json loading
            kwargs['init'] = PostgresBackend.initialize
        return await create_pool(*args, **kwargs)

    @staticmethod
    async def initialize(connection):
        await connection.set_type_codec(
            "json", encoder=json.dumps, decoder=json.loads, schema="pg_catalog"
        )

    @staticmethod
    async def connect(*args, **kwargs):
        connection = await connect(*args, **kwargs)
        await PostgresBackend.initialize(connection)
        return connection
