# adbc

`adbc` (short for **A**synchronous **D**ata**B**ase **C**onnector) is a library and CLI that provides high-level abstractions for comparing and copying databases.

## Support

`adbc` currently support Postgres only, Redshift, MySQL, and SQLite backends are in progress
- [x] Postgres (asyncpg)
- [ ] Redshift (WIP)
- [ ] MySQL (WIP)
- [ ] SQLite (WIP)

## Installation

`adbc` is packaged and distributed via PyPi and can be installed with any Python package manager:

### poetry

1. Run `poetry install adbc`

## pipenv

1. Run `pipenv install adbc`

## pip

1. Run `pip install adbc` within a virtualenv or `sudo pip install adbc` to install globally

## Getting Started

`adbc` provides abstractions at several levels:
- [PreQL](docs/preql.md): compile JSON queries into SQL
- [Databases](#databases): executing queries, copying data, introspection
- [Workflows](#workflows): querying, diffing, copying, and reading statistics from databases

The easiest way to work with `adbc` is by creating a [config file](#config-file) and populating it with a database and workflow, like this:

``` yaml
databases:
    test:
        url: postgres://localhost/postgres
workflows:
    test-info:
        steps:
            - type: info
              source: test
```

You can then run the workflow with `adbc run test-info`

## Config File

`adbc` looks for a config file ("adbc.yml" by default) where it expects to find:

``` yaml
adbc:                                   # adbc metadata
    version: string                         # tool version
databases:                              # database definitions
    name:                                   # database name
        url: string                             # database URL
        scope: ?object                          # database scope
        prompt: ?boolean                        # database calls require prompt
workflows:                              # workflow definitions
    name:                                   # workflow name
        verbose: ?integer                       # verbosity level
        steps:                                  # step list
        - type: query                             # run a SQL/PreQL query
          source: string                            # database name
          query: string                             # query to run
        - type: info                              # get info about a database
          source: string                            # database name
          scope: ?object                            # scope to a subset of the data
          schema: boolean                           # include schema information (default: True)
          data: boolean                             # include data information (default: True)
          hashes: boolean                           # include data hash information (default: True)
        - type: diff                              # compare two databases
          source: string                            # database name
          target: string                            # other database name
          scope: ?object                            # scope to a subset of the data
          schema: boolean                           # include schema information (default: True)
          data: boolean                             # include data information (default: True)
          hashes: boolean                           # include data hash information (default: True)
        - type: copy                              # copy a database into another
          source: string                            # database name
          target: string                            # other database name
          scope: ?object                            # scope to a subset of the data
```

## Databases

Databases are the central abstraction in `adbc`, each representing a distinct datastore at a network or file location.
They provide the following features:

1. Async query execution: run queries at different levels of abstractions:
  - parameterized SQL queries
``` python
    query = (
        "SELECT users.name, count(user_groups.id) AS num_groups "
        "FROM users "
        "LEFT JOIN user_groups ON user_groups.user_id = users.id "
        "WHERE users.country = $1 "
        "GROUP BY users.id ",
        'USA'
    )

    # execute
    value = await database.execute(*query);
    print(value) #  [{"name": "jay", "num_groups": 2}]

    # stream
    value = {}
    async for row in database.stream(*query):
        value[row['name']] = row['num_groups']
    print(value)  # {"jay": 2}

    # query_one_row
    value = await database.query_one_row(*query)
    print(value)  # {"name": "jay", "num_groups": 2}

    # query_one_value
    value = await database.query_one_value(query, 101);
    print(value) #  "jay"

```
  - PreQL queries
``` python
    query = {
        "select": {
            "values": {
                "name": "users.name",
                "num_groups": {
                    "count": "user_groups.id"
                }
            },
            "from": "users",
            "join": {
                "user_groups": {
                    "to": "user_groups",
                    "type": "left",
                    "on": {
                        "=": ["user_groups.user_id", "users.id"]
                    }
                }
            },
            "group": "users.id",
            "where": {"=": ["id", 101]}
        }
    }

    # execute
    value = await database.execute(preql=query)
    print(value) #  [{"name": "jay"}]

    # stream, query_one_row, query_one_value
    # ... (same, as execute, pass in preql=query)
```
  - model-oriented queries
``` python
    # get_model
    model = await database.get_model('users')

    # where, take
    query = model.where(id=101).take('name')

    # get
    value = await query.get()
    print(value) #  [{"name": "jay"}]
```
2. Schema introspection: identify all schematic elements of your database:
``` python
    # get_info
    info = await database.get_info()
    print(info)

    # get_children
    namespaces = await database.get_children()
    tables = await namespaces[0].get_children()
```

3. Diff: compare schema and data between two different databases
``` python
    # diff
    diff = await database.diff(other_database)
    print(diff)
```

4. Copy: sync schema and data between two different databases
``` python
    # copy
    copy = await database.copy(other_database)
    print(copy)
```

## Workflows

Workflows provide a high-level interface for defining multi-database operations in a sequence of steps. A workflow has a name and one or more steps of the following types:

### info

An **info** workflow obtains schema/data information from a source database.

### diff

A diff workflow obtains schema/data information from source and target databases, then compares the two.

### copy

A copy workflow syncs schema and data information from a source database into a target database.

### query

A query workflow runs a query against a source database. This can be used to return information or perform an update or schema change.

## Use Cases

`adbc` workflows enable a few common use-cases:

### Lazy Replication

**What?** copying one database to another in a stateless and interruptible way

**How?** This type of replication is implemented by the function `Database.copy` and the *copy* workflow step

### Fingerprinting

**What?** Capturing a sample of a database that can be used for comparison

**How?** Fingerprinting is implemented by `Database.get_info` and the *info* step

### Reverse ORM

**What?** interact with an unknown database using an ORM

**How?** This is made by possible by the *introspection* feature of databases

### Cross-database denormalization

**What?** updating a table with data from another database

**How?** See [this test](tests/integration/test_workflow.py) for an implementation example
