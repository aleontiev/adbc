from adbc.preql import build
from adbc.preql.dialect import Dialect, Backend, ParameterStyle


def get_dialect():
    return Dialect(
        backend=Backend.POSTGRES, style=ParameterStyle.FORMAT
    )

def test_build_create():
    dialect = get_dialect()
    expectations = [
        ( # 1. create one database
            {
                'create': {
                    'database': 'test'
                }
            },
            [('CREATE DATABASE "test"', [])]
        ),
        ( # 2. create two schemas
            {
                'create': [{
                    'schema': 'one',
                }, {
                    'schema': 'two'
                }]
            },
            [
                ('CREATE SCHEMA "one"', []),
                ('CREATE SCHEMA "two"', [])
            ]
        ),
        (
            {
                'create': {
                    'table': {
                        'name': 'one.test',
                        'columns': [{
                            'name': 'id',
                            'type': 'int',
                            'null': False,
                        }, {
                            'name': 'name',
                            'type': 'text',
                            'null': True
                        }],
                        'constraints': [{
                            'type': 'p',
                            'columns': ['id'],
                            'name': 'test__id__pk',
                        }, {
                            'type': 'c',
                            'name': 'test__id__name__check',
                            'check': {'!=': ['name', 'id']},
                            'deferrable': True,
                            'deferred': True
                        }],
                    }
                }
            },
            [
                (
                    'CREATE TABLE "one"."test" (\n'
                    '    "id" int NOT NULL,\n'
                    '    "name" text,\n'
                    '    CONSTRAINT "test__id__pk" PRIMARY KEY ("id") NOT DEFERRABLE INITIALLY IMMEDIATE\n'
                    '    CONSTRAINT "test__id__name__check" CHECK ("name" != "id") DEFERRABLE INITIALLY DEFERRED\n'
                    ')', []
                )
            ]
        )
    ]

    for query, result in expectations:
        assert build(query, dialect=dialect) == result
