## PreQL

In order to support different database backends and allow for user generation of safe queries, `adbc` includes a query compiler that converts from PreQL-formatted JSON into SQL.

PreQL is similar in function to SQLAlchemy but is easier to port across languages and generate with various tools because it is based on JSON.

The intuition behind PreQL is to consider an infix representation of a parsed SQL query.
The query is represented as an object with a single key corresponding to a command (SELECT, UPDATE, INSERT, DELETE, ALTER TABLE, etc)
This query object can have other objects inside that represents clauses, expressions, identifiers, and literals.

### Definitions

A **statement** object represents a SQL query or statement, e.g. an entire SELECT statement with all of its clauses
A **clause** object or array represents a part of a query, e.g. a SELECT statement has clauses for WHERE, FROM, GROUP BY, etc
An **expression** object represents a SQL expression more complicated than a literal or identifier
An **identifier** is a string that represents a column, table, schema, database, or user.
A **literal** is a SQL user string, boolean, or number


### Schema

PreQL can be validated with this JSON Schema:

TODO
```
{
}
```

### Statements

#### select

Represents a SELECT statement

Example:
```
{
    "select": {
        "with": {...},
        "return": {
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
        "join": {
            "profile": {
                "to": "public.profile",
                "on": {
                    "=": ["profile.user_id", "user.id"]
                }
            },
            "group_users": {
                "type": "left",
                "to": "public.group_users",
                "on": {
                    "=": ["group_users.user_id", "users.id"]
                }
            }
        },
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
        }],
        "limit": 100,
        "offset": 10,
    }
}
```
#### insert

Represents an INSERT statement

Example:
```
{
    "insert": {
        "into": "films",
        "columns": ["id", "name"]
        "values": [[1, "test"]],
        "return": {...}
    }
}
```

#### update

Represents an UPDATE statement

Example:
```
{
    "update": {
        "with": {...},
        "into": {...},
        "set": {
            "a": "`DEFAULT`",
            "b": {"concat": ["u.x", "u.y"]}
        },
        "where": {...},
        "return": {...}
    }
}
```

#### delete

Represents a DELETE statement

Example:
```
{
    "delete": {
        "with": {...}
        "from": {...},
        "where": {...},
        "returning": {...}
    }
}
```

#### truncate

Represents a TRUNCATE statement

Example:
```
{
    "truncate": "table"
}
```

#### create database

Represents a CREATE DATABASE statement

Example:
```
{       
    "create database": {
        "name": "test",
        "owner": "user",
        "encoding": "utf-8"
    }
}
```

#### create schema
```
{
    "create schema": {
        "name": "test"
    }
}
```

#### create table
```
{
    "create table": {
        "name": "...",
        "columns": {...}
        "constraints": {...},
        "indexes": {...}
    }
}
```

#### create sequence

#### create index

#### drop database

#### drop schema

#### drop table

#### drop sequence

#### drop index
