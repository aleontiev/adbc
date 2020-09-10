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
                            'sequence': True,  # automatic autonamed sequence
                            'null': False,
                            'primary': 'pk'  # automatic named constraint
                        }, {
                            'name': 'location_id',
                            'type': 'int',
                            'related': {  # automatic FK
                                'to': 'locations',
                                'by': 'id'
                            }
                        }, {
                            'name': 'name',
                            'type': 'text',
                            'unique': True,  # automatic autonamed constraint
                            'null': True
                        }],
                        'constraints': [{ # composite constraint
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
                        }, { # composite index
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
                    '    CONSTRAINT "ck" CHECK ("name" != "id") DEFERRABLE INITIALLY DEFERRED,\n'
                    '    CONSTRAINT "pk" PRIMARY KEY ("id") NOT DEFERRABLE INITIALLY IMMEDIATE,\n'
                    '    CONSTRAINT "test__location_id__fk" FOREIGN KEY ("location_id") REFERENCES "locations" ("id") NOT DEFERRABLE INITIALLY IMMEDIATE,\n'  # noqa
                    '    CONSTRAINT "test__name__uk" UNIQUE ("name") NOT DEFERRABLE INITIALLY IMMEDIATE\n'
                    ')', []
                ),
                (
                    'CREATE SEQUENCE IF NOT EXISTS "one"."test__id__seq" OWNED BY "one"."test"."id"', []
                ),
                (
                    'ALTER TABLE "one"."test" ALTER COLUMN "id" SET DEFAULT nextval(\'one.test__id__seq\')', []
                ),
                ('CREATE INDEX "composite" ON "one"."test" USING hash ("id", "name")', [])
            ],
        ),
        (  # 4. create table as (select ...)
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
        ( # 5. create column + constraint + sequence
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
                    '    ADD COLUMN "name" text DEFAULT %s,\n'
                    '    ADD COLUMN "created" timestamp NOT NULL DEFAULT now(),\n'
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
                        "rename": "test2",
                        "add": {
                            "column": [{
                                "name": "created",
                                "type": "timestamp",
                                "null": False
                            }, {
                                "name": "updated",
                                "type": "timestamp",
                                "null": False,
                            }],
                            "constraint": {
                                "name": "updated_gte_created",
                                "type": "check",
                                "check": {">=": ["updated", "created"]}
                            }
                        },
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
                '    ADD COLUMN "created" timestamp NOT NULL,\n'
                '    ADD COLUMN "updated" timestamp NOT NULL,\n'
                '    ADD CONSTRAINT "updated_gte_created" CHECK ("updated" >= "created") NOT DEFERRABLE INITIALLY IMMEDIATE,\n'
                '    DROP COLUMN "first_name",\n'
                '    DROP COLUMN "last_name",\n'
                '    ALTER COLUMN "name" TYPE varchar(1024), ALTER COLUMN "name" DROP DEFAULT, ALTER COLUMN "name" DROP NOT NULL,\n'
                '    ALTER CONSTRAINT "name_unique" DEFERRABLE',
                []
            ), (
                'ALTER TABLE "test" RENAME COLUMN "name" TO "full_name"',
                []
            ), (
                'ALTER TABLE "test" RENAME TO "test2"',
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
                '    ALTER COLUMN "name" SET DEFAULT %s,\n'
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

def test_build_delete():
    dialect = get_dialect()
    expectations = [
        (
            {
                "delete": {
                    "table": "testing.test",
                    "where": {
                        "=": ["name", '"foo"']
                    },
                    "returning": ["id", "name"]
                }
            },
            [(
                'DELETE FROM "testing"."test"\n'
                'WHERE "name" = %s\n'
                'RETURNING "id", "name"', ['foo']
            )]
        ), (
            {
                "delete": "test"
            },
            [('DELETE FROM "test"', [])]
        )
    ]
    for query, expected in expectations:
        result = build(query, dialect=dialect)
        assert expected == result


def test_build_update():
    dialect = get_dialect()
    expectations = [
        (
            {  # 1. simple update
                "update": {
                    "table": "testing.test",
                    "set": {
                        "name": '"bar"',
                        "toggled": True
                    },
                }
            },
            [(
                'UPDATE "testing"."test" SET\n'
                '    "name" = %s,\n'
                '    "toggled" = True',
                ['bar']
            )]
        ), (  # 2. update w/ subquery, where, with, returning
            {
                "update": {
                    "table": "testing.test",
                    "with": {
                        "query": {
                            "select": {
                                "data": "*",
                                "from": "bar.foo"
                            }
                        },
                        "as": "foo"
                    },
                    "set": [
                        "id",
                        "updated",
                        {
                            "select": {
                                "data": ["id", "updated"],
                                "from": "other",
                                "where": {
                                    "=": ["name", "other.name"]
                                }
                            }
                        }
                    ],
                    "where": {
                        "=": ["name", '"foo"']
                    },
                    "returning": [
                        "id",
                        {"name": {"concat": ["first_name", "last_name"]}}
                    ]
                }
            },
            [(
                'WITH "foo" AS (\n'
                '    SELECT *\n'
                '    FROM "bar"."foo"\n'
                ')\n'
                'UPDATE "testing"."test" SET\n'
                '    ("id", "updated") = (\n'
                '        SELECT\n'
                '            "id",\n'
                '            "updated"\n'
                '        FROM "other"\n'
                '        WHERE "name" = "other"."name"\n'
                '    )\n'
                'WHERE "name" = %s\n'
                'RETURNING "id", concat("first_name", "last_name") AS "name"', ['foo']
            )]
        )
    ]
    for query, expected in expectations:
        result = build(query, dialect=dialect)
        assert expected == result


def test_build_insert():
    dialect = get_dialect()
    expectations = [
        (
            {  # 1. default insert, no values
                "insert": "testing.test"
            },
            [(
                'INSERT INTO "testing"."test" DEFAULT VALUES'
                []
            )]
        ), (  # 2. insert one row with values
            {
                "insert": {
                    "table": "testing.user",
                    "values": ["'jim'", "'jim@test.com'"]
                }
            },
            [(
                'INSERT INTO "testing"."test" VALUES\n'
                '    (%s, %s)', ['jim', 'jim@test.com']
            )]
        ), (  # 3. insert many rows, columns, default values
            {
                'insert': {
                    'table': 'testing.user',
                    'columns': ['name', 'email'],
                    "values": [
                        ["'jim'", "'jim@test.com'"],
                        ["'jane'", {"DEFAULT": None}]
                    ]
                }
            },
            [(
                'INSERT INTO "testing"."test" ("name", "email") VALUES\n'
                '    (%s, %s)\n'
                '    (%s, DEFAULT)\n', ['jim', 'jim@test.com', 'jane']
            )]
        ), (  # 4. insert with query
            {
                "insert": {
                    "table": "testing.user",
                    "values": {
                        "select": {
                            "data": "name",
                            "from": "other.user"
                        }
                    }
                }
            },
            [(
                'INSERT INTO "testing"."test"\n'
                '    SELECT "name"\n'
                '    FROM "other"."user"', []
            )]
        )
    ]
    for query, expected in expectations:
        result = build(query, dialect=dialect)
        assert expected == result


def test_build_truncate():
    dialect = get_dialect()
    expectations = [
        (
            {  # 1. simple truncate
                "truncate": "test"
            },
            [(
                'TRUNCATE "test"',
                []
            )]
        ), (  # 2. truncate many
            {
                "truncate": [
                    "test",
                    {
                        "name": "other",
                        "cascade": True
                    }
                ]
            },
            [(
                'TRUNCATE "test"', []
            ), (
                'TRUNCATE "other" CASCADE', []
            )]
        )
    ]
    for query, expected in expectations:
        result = build(query, dialect=dialect)
        assert expected == result


def test_build_drop():
    dialect = get_dialect()
    expectations = [
        (
            {  # 1. single drop
                "drop": {
                    "table": {
                        "name": "test",
                        "cascade": True,
                        "maybe": True
                    }
                }
            },
            [(
                'DROP TABLE IF EXISTS "test" CASCADE',
                []
            )]
        ), (  # 2. multiple drop
            {
                "drop": [
                    {"table": "test"},
                    {"column": "other.id"},
                    {"index": "index"},
                    {"schema": {"name": "public"}},
                    {"constraint": ["other.pk", "this.pk"]}
                ]
            },
            [(
                'DROP TABLE "test"', []
            ), (
                'ALTER TABLE "other" DROP COLUMN "id"', []
            ), (
                'DROP INDEX "index"', []
            ), (
                'DROP SCHEMA "public"', []
            ), (
                'ALTER TABLE "other" DROP CONSTRAINT "pk"', []
            ), (
                'ALTER TABLE "this" DROP CONSTRAINT "pk"', []
            )]
        )
    ]
    for query, expected in expectations:
        result = build(query, dialect=dialect)
        assert expected == result


def test_build_select():
    # already tested by create and update+with
    pass
