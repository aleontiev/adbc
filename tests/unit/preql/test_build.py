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
                    '    CONSTRAINT "test__id__pk" PRIMARY KEY ("id") NOT DEFERRABLE INITIALLY IMMEDIATE,\n'
                    '    CONSTRAINT "test__id__name__check" CHECK ("name" != "id") DEFERRABLE INITIALLY DEFERRED\n'
                    ')', []
                )
            ],
        ),
        (
                {
                    'create': {
                        'table': {
                            'name': 'one.test',
                            'as': {
                                'select': {
                                    'data': [
                                        'id',
                                        'name',
                                        {
                                            'age': {
                                                'age': [
                                                    {'now': []},
                                                    'birthday'
                                                ]
                                            },
                                            'num_groups': {
                                                'count': 'groups.id'
                                            },
                                        }
                                    ],
                                    'from': 'users',
                                    'join': [{
                                        'to': 'user_groups',
                                        'type': 'left',
                                        'as': 'ug',
                                        'on': {
                                            '=': [
                                                'ug.user_id',
                                                'users.id'
                                            ]
                                        }
                                    }, {
                                        'to': 'groups',
                                        'type': 'left',
                                        'on': {
                                            '=': [
                                                'groups.id',
                                                'ug.group_id'
                                            ]
                                        }
                                    }],
                                    'where': {
                                        'or': [{
                                            'like': [
                                                'user.email',
                                                '"foo.com"'
                                            ]
                                        }, {
                                            '=': [
                                                'user.is_active',
                                                True
                                            ],
                                        }]
                                    },
                                    'group': {
                                        'by': 'users.id'
                                    },
                                    'having': {
                                        '>': ['num_groups', 3]
                                    },
                                    'limit': 1
                                }
                            }
                        }
                    }
                },
                [
                    (
                        'CREATE TABLE "one"."test" AS (\n'
                        '    SELECT\n'
                        '        "id",\n'
                        '        "name",\n'
                        '        age(now(), "birthday") AS "age",\n'
                        '        count("groups"."id") AS "num_groups"\n'
                        '    FROM "users"\n'
                        '    LEFT JOIN "user_groups" AS "ug" ON "ug"."user_id" = "users"."id"\n'
                        '    LEFT JOIN "groups" ON "groups"."id" = "ug"."group_id"\n'
                        '    WHERE ("user"."email" like %s) or ("user"."is_active" = True)\n'
                        '    GROUP BY "users"."id"\n'
                        '    HAVING "num_groups" > 3\n'
                        '    LIMIT 1\n'
                        ')',
                        ['foo.com']
                    )
                ]
        )
    ]

    for query, expected in expectations:
        result = build(query, dialect=dialect)
        assert expected == result
