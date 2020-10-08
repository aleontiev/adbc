## ZQL

In order to support different database backends and allow for user generation of safe queries, `adbc` includes a query compiler that converts from ZQL-formatted JSON into SQL.

ZQL is similar in function to SQLAlchemy but is easier to port across languages and generate with various tools because it is based on JSON.

### Definitions

- A ZQL **backend** is an RDBMS-specific compiler that renders ZQL into SQL and parameters
- A ZQL **style** is a setting that determins how parameterized queries should be constructed
- A ZQL **dialect** is a combination of backend and style which can be used by a specific downstream RDBMS driver
- A ZQL **statement** is a object that represents a SQL statement, e.g. an entire SELECT statement with all of its clauses
- A ZQL **clause** is an object or array that represents part of a statement, e.g. the FROM clause in a SELECT statement
- An **expression** is a ZQL object that represents a SQL expression more complicated than a literal or identifier, e.g. a function call or operator
- A **literal** refers to a SQL literal: string, boolean, or number
- An **identifier** is a string that represents a column, table, schema, database, constraint, index, or user

### Supported Backends

- Postgres
- MySQL (in progress)
- SQLite (in progress)

### Supported Styles

ZQL supports many parameter styles to accomodate all of the different SQL drivers (e.g. `asyncpg`, `psycopg2`, `libmysql`, `sqlite3`)

- Numeric (e.g. "SELECT * FROM user WHERE id = :1")
- Dollar Numeric (e.g. "SELECT * FROM user WHERE id = $1")
- Question Mark (e.g. "SELECT * FROM user WHERE id = ?")
- Format (e.g. "SELECT * FROM user WHERE id = %s")
- Named (e.g. "SELECT * FROM user WHERE id = :id")
- Dollar Named (e.g. "SELECT * FROM user WHERE id = $id")

### Structure

The intuition behind the structure of a ZQL query is to consider an infix representation of a SQL query:

- The query is represented as a single-key object (SKO) with the name of a command (e.g. `SELECT`, `UPDATE`, `INSERT`, `DELETE`, `ALTER TABLE`, etc)
- This query object can have other objects inside that represents expressions, including clauses, sub-queries, functions, keywords, operators, identifiers, and literals
- Clauses are represented by the data in specific keys of the query object (e.g. `with`, `from`, `data`, `return`)
- Sub-queries are represented by a SKO with the command name as the key (e.g. `select`)
- Functions are represented by a SKO with the function name as the key, and an array of arguments or non-array (interpretted as the sole argument) (e.g. `md5`, `concat`)
- Keywords are represented by a SKO with the function name as the key and "null" as the value (e.g. `default`)
- Special operators (3+ operands or clausal) are represented by a SKO with the clause name as the key (e.g. `case`, `between`) and an object of arguments specific to each clause type
- Normal operators are represented by a SKO with the operator as the key (e.g. `=`, `LIKE`, `NOT`) and an array of arguments or single argument for unary operators
- Identifiers are represented by strings without any special quoting, with possible dot characters (`.`) indicating separation between identifier parts (e.g. `"public.user"` represents the table user in the schema public)
- Literal booleans or numbers are represented as-is (e.g. `1.1`, `True`)
- Literal strings are represented by strings with an initial character and final character both equal to single quote (`'`) or double quote (`"`) (e.g. `"john o'conner"` represents the literal "john o'conner")
- Escaped literal strings are represented by strings with an initial character and final character both equal to tick mark (`) and are mostly identical to literals

### Caveats

- Some ZQL queries may actually require multiple SQL queries (e.g. `alter` or `create` with array arguments), so ZQL always returns an array of query results (even though this is not very intuitive for `select`)
- Even though all queries produced by ZQL will be valid SQL, not all queries will execute on all backends; for a trivial example, consider a custom user-defined function "foo" which is valid to specify in ZQL/SQL but will not successfully execute elsewhere unless "foo" is defined there as well

### SQL Injection

To provide safety from SQL injection, ZQL ensures the following:
- Identifiers are always escaped with identifier quote characters (e.g. `"` in Postgres)
- Normal literal strings are not escaped by ZQL and will instead by returned separately as parameters to later be interpretted by a SQL driver
- Escaped literal strings are escaped by ZQL using SQL-standard escaping (doubling quote characters inside the literal)
- The names of all keywords and functions are validated for alphanumeric characters
- The names of operators are validated by cross-checking a known list
- It is not possible to add arbitrary and unvalidated raw SQL in ZQL

This means that, even if working with unvalidated user input, ZQL queries should be protected from injection.

### Statements

#### alter database

TODO

#### alter schema

TODO

#### alter table

TODO

#### alter sequence

TODO

#### alter column

TODO

#### alter constraint

TODO

#### alter index

TODO

#### create database

Represents a CREATE DATABASE statement

ZQL:
```
{       
    "create": {
        "database": {
            "maybe": True,
            "name": "test",
            "owner": "user",
            "encoding": "utf-8"
        }
    }
}
```

SQL (Postgres):
```
[(
    'CREATE DATABASE IF NOT EXISTS "test" OWNER "user" ENCODING utf-8', []
)]
```

#### create schema

Represents a CREATE SCHEMA statement

ZQL:
```
{
    "create": {
        "schema": {
            "maybe": True,
            "name": "test"
        }
    }
}
```

SQL (Postgres):
```
[(
    'CREATE SCHEMA IF NOT EXISTS "test"', []
)]
```

#### create table

Represents a CREATE TABLE statement

ZQL:
```
{
    "create": {
        "table": {
            "name": "user",
            "columns": {
                "id": {
                    "type": "integer",
                }
            }.
            "constraints": {
                "pk": {
                    "type": "primary",
                    "columns": ["id"],
                }
            },
            "indexes": {
                "idx": {
                    "type": "btree",
                    "columns": ["first_name", "last_name"]
                }
            }
        }
    }
}
```

SQL (Postgres):
```
[(
    'CREATE TABLE "user"'
    '    "id" integer,'
    '    PRIMARY KEY "id"', []
), (
    'CREATE INDEX "idx" ON "user" USING btree ("first_name", "last_name")', []
)]
```

#### create sequence

Represents one or more CREATE SEQUENCE statements

ZQL:
```
{
    "create": {
        "sequence": {
            "maybe": True,
            "name": "user_id_seq",
            "owned_by": "user.id",
            "min": 0,
            "max": 10000,
            "start": 1,
            "increment": 10
        }
    }
}
```

SQL (Postgres)
```
[(
    'CREATE SEQUENCE IF NOT EXISTS "user_id_seq" OWNED BY "user"."id" MIN 0 MAX 10000 START 1 INCREMENT BY 10', []
)]
```

#### create index

Represents one or more CREATE INDEX statements

ZQL:
```
{
    "create": {
        "index": {
            "maybe": True,
            "name": "user_cmp",
            "on": "user",
            "concurrently": True,
            "columns": ["first_name", "last_name"]
        }
    }
}
```

SQL (Postgres):
```
[(
    'CREATE INDEX CONCURRENTLY IF NOT EXISTS "user_cmp" ON "user" ("first_name", "last_name")', []
)]
```

#### create column

Represents a ALTER TABLE statement with ADD COLUMN

ZQL:
```
{
    "create": {
        "column": {
            "on": "user",
            "name": "updated",
            "type": "timestamp with time zone",
        }
    }
}
```

SQL (Postgres):
```
[(
    "ALTER TABLE "user" ADD COLUMN "updated" timestamp with time zone", []
)]
```

#### create multiple

It is possible to create many schematic elements at once by passing an array of objects to `create`.
Each element in this array is expected to match one of the above.

ZQL will automatically normalize the query into a minimum number of DDL statements necessary to execute the intent on the given backend.
For example, creating two columns on the same table will produce a single ALTER TABLE statement for most backends (unless they do not support it)

ZQL:
```
{
    "create": [{
        "column": {
            "on": "user",
            "name": "updated",
            "type": "timestamp with time zone",
            "default": {"now": {}}
        }
    }, {
        "column": {
            "on": "user",
            "name": "created",
            "type": "timestamp with time zone",
            "default": "'2000-01-01T00:00:00Z'"
        }
    }]
}
```

SQL (Postgres):

```
[(
    'ALTER TABLE "user"\n'
    '    ADD COLUMN "updated" timestamp with time zone DEFAULT now()\n'
    '    ADD COLUMN "created" timestamp with time zone DEFAULT $1\n',
    ["2000-01-01T00:00:00Z"]
)]
```

SQL (SQLite):
```
[(
    'ALTER TABLE "user" ADD COLUMN "updated" timestamp with time zone DEFAULT now()', []
), (
    'ALTER TABLE "user" ADD COLUMN "created" timestamp with time zone DEFAULT $1', ["2000-01-01T00:00:00Z"]
)]
```

#### drop database
TODO

#### drop schema
TODO

#### drop table
TODO

#### drop sequence
TODO

#### drop index
TODO

#### drop column

TODO

#### drop constraint

TODO

#### select

Represents a SELECT statement

ZQL:
```
{
    "select": {
        "with": {
            "query": {
                "select": {
                    "data": "name"
                    "from": "groups"
                }
            },
            "as": "group_counts"
        },
        "data": {
            "id": "user.id",
            "name": {
                "concat": [
                    "user.first_name",
                    "user.last_name"
                ]
            },
            "num_groups": {
                "count": "groups.id"
            }
        },
        "from": {
            "user": "public.user"
        },
        "join": [{
            "as": "profile",
            "to": "public.profile",
            "on": {
                "=": ["profile.user_id", "user.id"]
            }
        }, {
            "as": "group_users",
            "type": "left",
            "to": "public.group_users",
            "on": {
                "=": ["group_users.user_id", "users.id"]
            }
        }],
        "where": {
            "and": [{
                ">": ["profile.created", "'2018-01-01'"]
            }, {
                "<": ["profile.created", {"now": []}]
            }]
        },
        "group": [{
            "by": "user.id",
            "rollup": True
        }],
        "having": {
            ">": ["num_groups", 5]
        },
        "order": [{
            "by": "user.created"
            "desc": True
        }, "user.updated"],
        "limit": 100,
        "offset": 10,
    }
}
```

SQL:
TODO

#### insert

Represents an INSERT statement

ZQL:
```
{
    "insert": {
        "table": "films",
        "columns": ["name"]
        "values": [["a", "b"]],
        "return": ["id"]
    }
}
```

SQL:
TODO

#### update

Represents an UPDATE statement

ZQL:
```
{
    "update": {
        "table": {"u": "user"},
        "set": {
            "a": {"default": null},
            "b": {"concat": ["u.x", "u.y"]}
        },
        "with": {...},
        "where": {...},
        "return": {...},
    }
}
```

SQL (Postgres):
TODO

#### delete

Represents a DELETE statement

ZQL:
```
{
    "delete": {
        "table": "user",
        "with": {...}
        "where": {...},
        "return": {...}
    }
}
```

SQL (Postgres):
TODO

#### truncate

Represents a TRUNCATE statement

ZQL:
```
{
    "truncate": "user"
}
```

SQL (Postgres):
```
[(
    'TRUNCATE TABLE "user"', []
)]
```
