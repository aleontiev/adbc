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
        ( # 3. create table with columns, constraints, indexes
            {
                'create': {
                    'table': {
                        'name': 'one.test',
                        'columns': [{
                            'name': 'id',
                            'type': 'int',
                            'null': False,
                        }, {
                            'name': 'location_id',
                            'type': 'int',
                        }, {
                            'name': 'name',
                            'type': 'text',
                            'null': True
                        }],
                        'constraints': [{
                            'type': 'foreign key',
                            'columns': ['location_id'],
                            'related_name': 'locations',
                            'related_columns': ['id'],
                            'name': 'fk'
                        }, {
                            'type': 'primary key',
                            'columns': ['id'],
                            'name': 'pk',
                        }, {
                            'type': 'check',
                            'name': 'ck',
                            'check': {'!=': ['name', 'id']},
                            'deferrable': True,
                            'deferred': True
                        }],
                        'indexes': [{
                            'primary': True,
                            'name': 'pk',
                            'columns': ['id'],
                        }, {
                            'name': 'composite',
                            'type': 'hash',
                            'columns': ['id', 'name'],
                        }]
                    }
                }
            },
            [
                (
                    'CREATE TABLE "one"."test" (\n'
                    '    "id" int NOT NULL,\n'
                    '    "location_id" int,\n'
                    '    "name" text,\n'
                    '    CONSTRAINT "fk" FOREIGN KEY ("location_id") REFERENCES "locations" ("id") NOT DEFERRABLE INITIALLY IMMEDIATE,\n'  # noqa
                    '    CONSTRAINT "pk" PRIMARY KEY ("id") NOT DEFERRABLE INITIALLY IMMEDIATE,\n'
                    '    CONSTRAINT "ck" CHECK ("name" != "id") DEFERRABLE INITIALLY DEFERRED\n'
                    ')', []
                ),
                ('CREATE INDEX "composite" ON "one"."test" USING hash ("id", "name")', [])
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
        ),
        (
            {
                "create": [{
                    "column": [{
                        "name": "name",
                        "type": "text",
                        "on": "test",
                        "default": '"hello"'
                    }, {
                        "name": "created",
                        "null": False,
                        "type": "timestamp",
                        "on": "test",
                        "default": {"now": []}
                    }, {
                        "name": "name",
                        "type": "text",
                        "on": "test2"
                    }]
                }, {
                    "constraint": [{
                        "name": "check",
                        "type": "check",
                        "on": "test",
                        "check": {
                            '!=': ['name', "'test'"]
                        }
                    }]
                }, {
                    "sequence": {
                        "maybe": True,
                        "temporary": True,
                        "name": "public.test_id_seq",
                        "owned_by": "test.id",
                        "start": 100
                    }
                }]
            },
            [
                ('CREATE TEMPORARY SEQUENCE IF NOT EXISTS "public"."test_id_seq" START WITH 100 OWNED BY "test"."id"', []),
                (
                    'ALTER TABLE "test"\n'
                    '    ADD COLUMN "name" text DEFAULT %s\n'
                    '    ADD COLUMN "created" timestamp NOT NULL DEFAULT now()\n'
                    '    ADD CONSTRAINT "check" CHECK ("name" != %s) NOT DEFERRABLE INITIALLY IMMEDIATE',
                    ['hello', 'test'],
                ),
                ('ALTER TABLE "test2" ADD COLUMN "name" text', []),
            ],
        ),
    ]

    for query, expected in expectations:
        result = build(query, dialect=dialect)
        assert expected == result

def test_build_alter():
    dialect = get_dialect()
    expectations = [
        (
            {
                "alter": {
                    "table": {
                        "name": "test",
                        "add": [{
                            "column": [{
                                "name": "created",
                                "type": "timestamp",
                                "null": False
                            }, {
                                "name": "updated",
                                "type": "timestamp",
                                "null": False,
                            }]
                        }, {
                            "constraint": {
                                "name": "updated_gte_created",
                                "type": "check",
                                "check": {">=": ["updated", "created"]}
                            }
                        }],
                        "alter": {
                            "column": {
                                "name": "name",
                                "rename": "full_name",
                                "type": "varchar(1024)",
                                "default": None,
                                "null": True
                            },
                            "constraint": {
                                "name": "name_unique",
                                "deferrable": True
                            }
                        },
                        "drop": {
                            "column": ["first_name", "last_name"]
                        }
                    }
                }
            },
            [(
                'ALTER TABLE "test"\n'
                '    ADD COLUMN "created" timestamp NOT NULL\n'
                '    ADD COLUMN "updated" timestamp NOT NULL\n',
                '    ADD CONSTRAINT "updated_gte_created" CHECK ("updated" >= "created") NOT DEFERRABLE INITIALLY IMMEDIATE\n'
                '    ALTER COLUMN "name" TYPE varchar(1024)\n',
                '    ALTER COLUMN "name" DROP DEFAULT\n'
                '    ALTER COLUMN "name" SET NOT NULL\n'
                '    ALTER CONSTRAINT "name_unique" DEFERRABLE\n'
                '    DROP COLUMN "first_name"\n'
                '    DROP COLUMN "last_name"',
                []
            ), (
                'ALTER TABLE "test" RENAME COLUMN "name" TO "full_name"',
                []
            )]
        ),
        (
            {
                "alter": [{
                    "column": {
                        "name": "name",
                        "on": "test",
                        "rename": "name2",
                        "default": "'test'"
                    }
                }, {
                    "constraint": {
                        "on": "test",
                        "name": "name_unique",
                        "deferrable": True
                    }
                }]
            }, [(
                'ALTER TABLE "test"\n'
                '    ALTER COLUMN "name" SET DEFAULT %s\n'
                '    ALTER CONSTRAINT "name_unique" DEFERRABLE',
                ['test']
            ), (
                'ALTER TABLE "test" RENAME COLUMN "name" TO "name2"',
                []
            )]
        )
    ]
    for query, expected in expectations:
        result = build(query, dialect=dialect)
        assert expected == result
