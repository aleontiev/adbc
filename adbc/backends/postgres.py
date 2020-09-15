import re
import json
import ssl

from typing import Union
from .base import DatabaseBackend
from cached_property import cached_property
from asyncpg import create_pool, connect
from urllib.parse import urlparse, parse_qs, urlencode
from adbc.preql.dialect import Dialect, Backend, ParameterStyle
from adbc.preql import parse, build


EMPTY_CLAUSE = {'=': [1, 1]}
TAGGED_NUMBER_REGEX = re.compile(r'[a-zA-Z]+ ([0-9]+)')



class PostgresBackend(DatabaseBackend):
    """Postgres backend based on asyncpg"""

    has_json_aggregation = True
    default_schema = 'public'
    dialect = Dialect(
        backend=Backend.POSTGRES,
        style=ParameterStyle.DOLLAR_NUMERIC
    )

    def build(self, query: Union[dict, list]):
        return build(query, dialect=self.dialect)

    def parse_expression(self, expression: str):
        """Return parsed PreQL expression"""
        return parse(expression, Backend.POSTGRES)

    async def copy_to_table(self, connection, table_name, **kwargs):
        result = await connection.copy_to_table(table_name, **kwargs)
        return self.get_tagged_number(result)

    async def copy_from_table(self, connection, table_name, **kwargs):
        result = await connection.copy_from_table(table_name, **kwargs)
        return self.get_tagged_number(result)

    async def copy_from_query(self, connection, query, params=None, **kwargs):
        params = params or []
        result = await connection.copy_from_query(query, *params, **kwargs)
        return self.get_tagged_number(result)

    async def execute(self, connection, query, params=None):
        params = params or []
        return await connection.execute(query, *params)

    async def cursor(self, connection, query, params=None):
        params = params or []
        async for x in connection.cursor(query, *params):
            yield x

    async def fetch(self, connection, query, params=None):
        params = params or []
        return await connection.fetch(query, *params)

    def get_tagged_number(self, value):
        match = TAGGED_NUMBER_REGEX.match(value)
        if not match:
            raise Exception('not a tagged number: {value}')

        return int(match.group(1))

    @staticmethod
    def get_databases_query(include, tag=None):
        table = "pg_database"
        column = "datname"

        where = PostgresBackend.get_include_preql(
            include, table, column, tag=tag
        ) or EMPTY_CLAUSE
        return {
            'select': {
                'data': column,
                'from': table,
                'where': {
                    'and': [{
                        '=': ['datistemplate', False]
                    }, where]
                }
            }
        }

    @staticmethod
    def get_tables_query(namespace, include, tag=None):
        table = "R"
        column = "relname"
        where = PostgresBackend.get_include_preql(
            include, table, column, tag=tag
        ) or EMPTY_CLAUSE

        columns = {
            'select': {
                'data': {
                    'name': 'R.relname',
                    'kind': {
                        'case': [{
                            'when': {
                                '=': ['R.relkind', '`r`']
                            },
                            'then': '`table`'
                        }, {
                            'when': {
                                '=': ['R.relkind', '`S`']
                            },
                            'then': '`sequence`'
                        }, {
                            'else': '`other`'
                        }]
                    },
                    'result': {
                        'json_agg': {
                            'json_build_object': [
                                '`name`',
                                'A.attname',
                                '`type`',
                                {
                                    'pg_catalog.format_type': [
                                        'A.atttypid', 'A.atttypmod'
                                    ]
                                },
                                "`default`",
                                {
                                    'pg_get_expr': ['D.adbin', 'D.adrelid']
                                },
                                "`null`",
                                {
                                    'not': 'A.attnotnull'
                                }
                            ]
                        }
                    }
                },
                'from': {
                    'A': 'pg_attribute'
                },
                'join': [{
                    'to': 'pg_class',
                    'as': 'R',
                    'on': {'=': ['R.oid', 'A.attrelid']}
                }, {
                    'to': 'pg_namespace',
                    'as': 'N',
                    'on': {'=': ['R.relnamespace', 'N.oid']}
                }, {
                    'type': 'left',
                    'to': 'pg_attrdef',
                    'as': 'D',
                    'on': {
                        'and': [{
                            '=': ['A.atthasdef', True]
                        }, {
                            '=': ['D.adrelid', 'R.oid']
                        }, {
                            '=': ['D.adnum', 'A.attnum']
                        }]
                    }
                }],
                'where': {
                    'and': [{
                        '>': ['A.attnum', 0]
                    }, {
                        'or': [{
                            '=': ['R.relkind', '`r`']
                        }, {
                            '=': ['R.relkind', '`S`']
                        }]
                    }, {
                        'not': 'A.attisdropped'
                    }, {
                        '=': ['N.nspname', f'"{namespace}"']
                    }, where]
                },
                'group': ['R.relname', 'R.relkind']
            }
        }
        constraints = {
            'select': {
                'data': {
                    'name': 'R.relname',
                    'result': {
                        'json_agg': {
                            'json_build_object': [
                                "`name`",
                                'C.conname',
                                "`deferrable`",
                                'C.condeferrable',
                                "`deferred`",
                                'C.condeferred',
                                "`type`",
                                {
                                    'case': [{
                                        'when': {'=': ['C.contype', "`c`"]},
                                        'then': "`check`"
                                    }, {
                                        'when': {'=': ['C.contype', "`f`"]},
                                        'then': "`foreign`"
                                    }, {
                                        'when': {'=': ['C.contype', "`p`"]},
                                        'then': "`primary`"
                                    }, {
                                        'when': {'=': ['C.contype', "`u`"]},
                                        'then': "`unique`"
                                    }, {
                                        'when': {'=': ['C.contype', "`t`"]},
                                        'then': "`trigger`"
                                    }, {
                                        'else': "`exclude`"
                                    }]
                                },
                                "`related_columns`",
                                {
                                    'array_to_json': {
                                        'array': {
                                            'select': {
                                                'data': 'attname',
                                                'from': 'pg_attribute',
                                                'join': {
                                                    'to': {
                                                        'select': {
                                                            'data': ['a', 'b'],
                                                            'from': {
                                                                'x': {
                                                                    'select': {
                                                                        'data': {
                                                                            'a': {
                                                                                'unnest': 'C.confkey'
                                                                            },
                                                                            'b': {
                                                                                'generate_series': [
                                                                                    1,
                                                                                    {
                                                                                        'array_length': ['C.confkey', 1]
                                                                                    }
                                                                                ]
                                                                            }
                                                                        }
                                                                    }
                                                                }
                                                            }
                                                        }
                                                    },
                                                    'as': 'ord',
                                                    'on': {'=': ['ord.a', 'pg_attribute.attnum']}
                                                },
                                                'where': {
                                                    'and': [{
                                                        '=': ['pg_attribute.attnum', {'any': 'C.confkey'}]
                                                    }, {
                                                        '=': ['pg_attribute.attrelid', 'F.oid']
                                                    }]
                                                },
                                                'order': 'ord.b'
                                            }
                                        }
                                    }
                                },
                                "`columns`",
                                {
                                    'array_to_json': {
                                        'array': {
                                            'select': {
                                                'data': 'attname',
                                                'from': 'pg_attribute',
                                                'join': {
                                                    'to': {
                                                        'select': {
                                                            'data': ['a', 'b'],
                                                            'from': {
                                                                'x': {
                                                                    'select': {
                                                                        'data': {
                                                                            'a': {
                                                                                'unnest': 'C.conkey'
                                                                            },
                                                                            'b': {
                                                                                'generate_series': [
                                                                                    1,
                                                                                    {
                                                                                        'array_length': ['C.conkey', 1]
                                                                                    }
                                                                                ]
                                                                            }
                                                                        }
                                                                    }
                                                                }
                                                            }
                                                        }
                                                    },
                                                    'as': 'ord',
                                                    'on': {'=': ['ord.a', 'pg_attribute.attnum']}
                                                },
                                                'where': {
                                                    'and': [{
                                                        '=': ['pg_attribute.attnum', {'any': 'C.conkey'}]
                                                    }, {
                                                        '=': ['attrelid', 'R.oid']
                                                    }]
                                                },
                                                'order': 'ord.b'
                                            }
                                        }
                                    }
                                },
                                "`related_name`",
                                'F.relname',
                                "`check`",
                                'C.consrc'
                            ]
                        }
                    }
                },
                'from': {
                    'R': 'pg_class'
                },
                'join': [{
                    'to': 'pg_constraint',
                    'as': 'C',
                    'on': {'=': ['C.conrelid', 'R.oid']}
                }, {
                    'to': 'pg_namespace',
                    'as': 'N',
                    'on': {'=': ['N.oid', 'R.relnamespace']}
                }, {
                    'type': 'left',
                    'as': 'I',
                    'to': 'pg_class',
                    'on': {'=': ['C.conindid', 'I.oid']}
                }, {
                    'type': 'left',
                    'to': 'pg_class',
                    'as': 'F',
                    'on': {'=': ['F.oid', 'C.confrelid']}
                }],
                'where': {
                    'and': [{
                        '=': ['N.nspname', f'"{namespace}"']
                    }, {
                        '=': ['R.relkind', "`r`"]
                    }, where]
                },
                'group': 'R.relname'
            }
        }
        indexes = {
            'select': {
                'data': {
                    'name': 'R.relname',
                    'result': {
                        'json_agg': {
                            'json_build_object': [
                                '`name`',
                                'IR.relname',
                                '`type`',
                                'IA.amname',
                                '`primary`',
                                'I.indisprimary',
                                '`unique`',
                                'I.indisunique',
                                '`columns`',
                                {
                                    'array_to_json': {
                                        'array': {
                                            'select': {
                                                'data': 'attname',
                                                'from': 'pg_attribute',
                                                'join': {
                                                    'to': {
                                                        'select': {
                                                            'data': ['a', 'b'],
                                                            'from': {
                                                                'x': {
                                                                    'select': {
                                                                        'data': {
                                                                            'a': {
                                                                                'unnest': 'I.indkey'
                                                                            },
                                                                            'b': {
                                                                                'generate_series': [
                                                                                    1,
                                                                                    {
                                                                                        'array_length': ['I.indkey', 1]
                                                                                    }
                                                                                ]
                                                                            }
                                                                        }
                                                                    }
                                                                }
                                                            }
                                                        }
                                                    },
                                                    'as': 'ord',
                                                    'on': {'=': ['ord.a', 'pg_attribute.attnum']}
                                                },
                                                'where': {
                                                    'and': [{
                                                        '=': ['pg_attribute.attnum', {'any': 'I.indkey'}]
                                                    }, {
                                                        '=': ['attrelid', 'R.oid']
                                                    }]
                                                },
                                                'order': 'ord.b'
                                            }
                                        }
                                    }
                                }
                            ]
                        }
                    }
                },
                'from': {
                    'R': 'pg_class'
                },
                'join': [{
                    'to': 'pg_index',
                    'as': 'I',
                    'on': {'=': ['R.oid', 'I.indrelid']}
                }, {
                    'to': 'pg_class',
                    'as': 'IR',
                    'on': {'=': ['IR.oid', 'I.indexrelid']}
                }, {
                    'to': 'pg_namespace',
                    'as': 'N',
                    'on': {'=': ['N.oid', 'R.relnamespace']}
                }, {
                    'to': 'pg_am',
                    'type': 'left',
                    'as': 'IA',
                    'on': {'=': ['IA.oid', 'IR.relam']}
                }],
                'where': {
                    'and': [{
                        '=': ['N.nspname', f'"{namespace}"']
                    }, {
                        '=': ['R.relkind', "`r`"]
                    }, where]
                },
                'group': 'R.relname'
            }
        }
        query = {
            'select': {
                'data': {
                    'name': 'Columns.name',
                    'columns': 'Columns.result',
                    'constraints': 'Constraints.result',
                    'indexes': 'Indexes.result',
                    'type': 'Columns.kind'
                },
                'from': {
                    'Columns': columns
                },
                'join': [{
                    'type': 'left',
                    'as': 'Constraints',
                    'on': {'=': ['Constraints.name', 'Columns.name']},
                    'to': constraints
                }, {
                    'type': 'left',
                    'as': 'Indexes',
                    'on': {'=': ['Columns.name', 'Indexes.name']},
                    'to': indexes
                }],
            }
        }
        return query

    @staticmethod
    def get_namespaces_query(include, tag=None):
        table = "pg_namespace"
        column = "nspname"
        where = PostgresBackend.get_include_preql(
            include, table, column, tag=tag
        )
        query = {
            'select': {
                'data': column,
                'from': table,
                'where': where
            }
        }
        return query

    @staticmethod
    def get_version_query():
        return {'select': {'data': {'version': {'version': []}}}}

    @staticmethod
    def get_table_indexes_query(namespace, include, tag=None):
        table = "R"
        column = "relname"
        where = PostgresBackend.get_include_preql(include, table, column, tag=tag) or EMPTY_CLAUSE
        query = {
            'select': {
                'data': {
                    'name': 'R.relname',
                    'index': 'IR.relname',
                    'type': 'IA.amname',
                    'primary': 'I.indisprimary',
                    'unique': 'I.indisunique',
                    'def': {
                        'pg_get_indexdef': 'I.indexrelid'
                    }
                },
                'from': {'R': 'pg_class'},
                'join': [{
                    'to': 'pg_index',
                    'as': 'I',
                    'on': {'=': ['R.oid', 'I.indrelid']}
                }, {
                    'to': 'pg_class',
                    'as': 'IR',
                    'on': {'=': ['IR.oid', 'I.indexrelid']}
                }, {
                    'to': 'pg_namespace',
                    'as': 'N',
                    'on': {'=': ['N.oid', 'R.relnamespace']}
                }, {
                    'type': 'left',
                    'to': 'pg_am',
                    'as': 'IA',
                    'on': {'=': ['IA.oid', 'IR.relam']}
                }],
                'where': {
                    'and': [
                        {'=': ['N.nspname', f'"{namespace}"']},
                        {'=': ['R.relkind', '`r`']},
                        where
                    ]
                }
            }
        }
        return query

    @staticmethod
    def get_table_constraints_query(namespace, include, tag=None):
        table = "R"
        column = "relname"
        where = PostgresBackend.get_include_preql(
            include, table, column, tag=tag
        ) or EMPTY_CLAUSE
        query = {
            'select': {
                'data': {
                    'name': 'R.relname',
                    'constraint': 'C.conname',
                    'deferrable': 'C.condeferrable',
                    'deferred': 'C.condeferred',
                    'type': {
                        'case': [{
                            'when': {'=': ['C.contype', "`c`"]},
                            'then': "`check`"
                        }, {
                            'when': {'=': ['C.contype', "`f`"]},
                            'then': "`foreign`"
                        }, {
                            'when': {'=': ['C.contype', "`p`"]},
                            'then': "`primary`"
                        }, {
                            'when': {'=': ['C.contype', "`u`"]},
                            'then': "`unique`"
                        }, {
                            'when': {'=': ['C.contype', "`t`"]},
                            'then': "`trigger`"
                        }, {
                            'else': "`exclude`"
                        }]
                    },
                    'related_name': 'F.relname',
                    'check': 'C.consrc',
                    'related_columns': 'Rel.attname',
                    'columns': 'A.attname'
                },
                'from': {
                    'R': 'pg_class'
                },
                'join': [{
                    'to': 'pg_constraint',
                    'as': 'C',
                    'on': {'=': ['C.conrelid', 'R.oid']}
                }, {
                    'to': 'pg_namespace',
                    'as': 'N',
                    'on': {'=': ['N.oid', 'R.relnamespace']}
                }, {
                    'type': 'left',
                    'to': 'pg_class',
                    'as': 'F',
                    'on': {'=': ['F.oid', 'C.confrelid']}
                }, {
                    'type': 'left',
                    'to': 'pg_attribute',
                    'as': 'Rel',
                    'on': {
                        'and': [{
                            '=': ['F.oid', 'Rel.attrelid']
                        }, {
                            '=': ['Rel.attnum', {'any': 'C.confkey'}]
                        }]
                    }
                }, {
                    'type': 'left',
                    'to': 'pg_attribute',
                    'as': 'A',
                    'on': {
                        'and': [{
                            '=': ['R.oid', 'A.attrelid']
                        }, {
                            '=': ['A.attnum', {'any': 'C.conkey'}]
                        }]
                    }
                }],
                'where': {
                    'and': [
                        {'=': ['N.nspname', f'"{namespace}"']},
                        {'=': ['R.relkind', '`r`']},
                        where
                    ]
                }
            }
        }
        return query

    @staticmethod
    def get_table_columns_query(namespace, include, tag=None):
        table = "R"
        column = "relname"
        where = PostgresBackend.get_include_preql(
            include, table, column, tag=tag
        ) or EMPTY_CLAUSE
        query = {
            'select': {
                'data': {
                    'name': 'R.relname',
                    'kind': {
                        'case': [{
                            'when': {'=': ['R.relkind', '`r`']},
                            'then': '`table`'
                        }, {
                            'when': {'=': ['R.relkind', '`S`']},
                            'then': '`sequence`'
                        }, {
                            'else': '`other`'
                        }]
                    },
                    'column': 'A.attname',
                    'type': {
                        'pg_catalog.format_type': ['A.atttypid', 'A.atttypmid']
                    },
                    'default': {
                        'pg_get_expr': ['D.adbin', 'D.adrelid']
                    },
                    'null': {
                        'not': 'A.attnotnull'
                    }
                },
                'from': {
                    'A': 'pg_attribute'
                },
                'join': [{
                    'to': 'pg_class',
                    'as': 'R',
                    'on': {'=': ['R.oid', 'A.attrelid']}
                }, {
                    'to': 'pg_namespace',
                    'as': 'N',
                    'on': {'=': ['R.relnamespace', 'N.oid']}
                }, {
                    'to': 'pg_attrdef',
                    'type': 'left',
                    'as': 'D',
                    'on': {
                        'and': [{
                            '=': ['A.atthasdef', True]
                        }, {
                            '=': ['D.adrelid', 'R.oid']
                        }, {
                            '=': ['D.adnum', 'A.attnum']
                        }]
                    }
                }],
                'where': {
                    'and': [{
                        '=': ['N.nspname', f'"{namespace}"']
                    }, {
                        '>': ['A.attnum', 0]
                    }, {
                        'or': [{
                            '=': ['R.relkind', '`r`']
                        }, {
                            '=': ['R.relkind', '`S`']
                        }]
                    }, {
                        'not': 'A.attisdropped'
                    }, where]
                }
            }
        }
        return query

    @staticmethod
    async def create_pool(*args, **kwargs):
        if 'init' not in kwargs:
            # initialize connection with json loading
            kwargs['init'] = PostgresBackend.initialize
        if kwargs.pop('skip_ca_check', False):
            ctx = ssl.create_default_context(cafile='')
            ctx.check_hostname = False
            ctx.verify_mode = ssl.CERT_NONE
            kwargs['ssl'] = ctx
        else:
            dsn = kwargs.get('dsn', None)
            if dsn and 'sslrootcert' in dsn:
                # asyncpg bug: the rootcert must be passed as a relative path
                # e.g. sslrootcert=rds-bundle.pem will not attempt to use the
                # rds-bundle.pem file from the current directory
                parsed = urlparse(dsn)
                query = parse_qs(parsed.query)
                cafile = query.pop('sslrootcert')[0]
                query = urlencode(query)
                query = f'?{query}' if query else ''
                dsn = f'{parsed.scheme}://{parsed.netloc}{parsed.path}{query}'
                if not cafile.startswith('.') and not cafile.startswith('/'):
                    # assume relative if starting with normal character
                    cafile = f'./{cafile}'
                ctx = ssl.create_default_context(cafile=cafile)
                kwargs['dsn'] = dsn
                kwargs['ssl'] = ctx
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
