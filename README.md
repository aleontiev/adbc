# adbc

`adbc` (short for **A**synchronous **D**ata**B**ase **C**onnector) is a library and CLI that provides high-level abstractions for querying, comparing, and copying databases.

## Support

`adbc` currently support Postgres only; Redshift, MySQL, and SQLite backends are in progress:
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
        verbose: ?[boolean, integer]            # verbosity
        steps:                                  # step list
        - type: query                             # run a SQL/PreQL query
          source: string                            # database name
          query: [string, object, list]             # query string or PreQL object or list
        - type: info                              # get info about a database
          source: string                            # database name
          scope: ?object                            # scope the data
          schema: ?boolean                          # include schema information (default: True)
          data: ?boolean                            # include data information (default: True)
          hashes: ?[boolean, integer]               # include data hash information + hash shard size (default: True)
        - type: diff                              # compare two databases
          source: string                            # origin database name
          target: string                            # other database name
          scope: ?object                            # scope the data
          schema: ?boolean                          # include schema information (default: True)
          data: ?boolean                            # include data information (default: True)
          hashes: ?[boolean, integer]               # include data hash information (default: True)
        - type: copy                              # copy a database into another
          source: string                            # database name
          target: string                            # other database name
          scope: ?object                            # scope to a subset of the data
```

## Databases

Databases are the central abstraction in `adbc`, each representing a distinct datastore identified primarily by either a local path or URI.
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
    )
    params = ['USA']

    # execute
    value = await database.execute(query, params);
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
            "data": {
                "name": "users.name",
                "num_groups": {
                    "count": "user_groups.id"
                }
            },
            "from": "users",
            "join": [{
                "as": "ug",
                "to": "user_groups",
                "type": "left",
                "on": {
                    "=": ["ug.user_id", "users.id"]
                }
            }],
            "group": "users.id",
            "where": {"=": ["country", "'USA'"]}
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

Workflows provide a high-level interface for defining multi-database operations in a sequence of steps.
A workflow has a name and one or more steps of the following types:

### Types

#### info

An **info** step extracts schema and/or statistics from a source database.
This provides the basis for diffing and copying.

#### diff

A **diff** step extracts schema and/or statistics from source and target databases, then compares the two.

#### copy

A **copy** step syncs schema and data from a source database into a target database.
This step performs **lazy replication**: syncing a target to a source on an arbitrary schedule without any change data capture or logical replication to rely on.
To do this efficiently, diffs are performed on the data in each table, with large tables split into shards.
This means that subsequent copies between the same source and target will take less time as only deltas must be applied.

#### query

A **query** step runs a query against a source database.
This can be used to query for arbitrary information or to perform data updates or schema changes.

### Scope

Except for `query`, all of the commands accept a `scope` parameter which defines the scope of interaction with the selected database.
For example, an `info` step can capture information about an entire database (if `scope` is not provided), or it can capture information about one or more schemas, or a specific table or set of tables.
Many of the `Database` methods also accept scope, notably `get_model`, `get_table`, and `get_children`.
The `Database` object also accepts an initial `scope` parameter, which is used as a default when scope is not provided.

#### null (default)

If `scope` is omitted or explicitly set to `null`, then the entire database is considered in scope for the operation.
This can be dangerous when used with the `copy` operation, because any namespaces or tables in the target that are not in the source will be deleted.

#### object

If `scope` is an object (dict), it is expected to contain a child object called `schemas`.
There are two types of keys in `schemas` which are referred to as selectors:

- Identifiers (e.g. "*", "name")
- Field selectors, prefixed by "&" (e.g. "&type")

Selectors are composed together with precedence given to specificity (less wildcards match with more precedence, identifier selectors match before field selectors)

This means you can define a scope which uses selectors like mixins for shorter and less redundant configuration, e.g:

``` yaml
scope:
    schemas:
        public:  # only the public schema, ignore other schemas
            '*': # apply this to all tables
                enabled: False  # not enabled
                constraints: 
                    '&type': # only primary key checks
                        unique: False  
                        check: False
                        foreign: False
            'auth_*': # sync these tables 
                enabled: True
            auth_user: 
                constraints: True  # all constraints
    
```
For multi-datasource operations `diff` and `copy`, `scope` can also be used to translate between schema, table, and column names. This is only allowed inside identifier selectors.

### Introspection

By default, a `Database` will attempt to use introspection to determine the schema of the database when a particular table is requested.
This is done by querying the "INFORMATION_SCHEMA" tables (or equivalent) which are provided by RDBMS systems as meta catalogs.
In order to avoid over-querying this information, `Database` uses an internal cache which is keyed by `scope` and table name.

It is possible to disable introspection by setting `introspect: False` in the root of the scope object.
When this happens, `Database` instances will not make any queries to determine metadata and will instead depend entirely on the data provided in the scope.
Only non-wildcard identifiers must be used, and all of a tables columns/constraints/indexes should be provided in the `scope`, as in this example:

``` yaml
scope:
    introspect: False
    schemas:
        public:
            auth_user:
                columns:
                    id:
                        type: int
                    first_name:
                        type: string
                    last_name:
                        type: string
                constraints:
                    pk:
                        type: primary
                indexes:
                    name_ck:
                        type: check
                        check:
                            '!=':
                                - first_name
                                - last_name
```

This feature can be used to provide fast ORM access to data based on JSON configuration, without incurring the cost of database queries to determine the schema.
However, it is enabled by default because the main intent of the library is to flexibly support any scope of work, including unscoped or partially scoped operations with wildcards.

## Use Cases

`adbc` workflows enable a few common use-cases:

### Lazy Replication

**What?** copying any subset of a database to another in a stateless and interruptible way

**How?** This type of replication is implemented by the function `Database.copy` and the *copy* workflow step

### Fingerprinting

**What?** Capturing a sample of a database that can be used as a reference point in a future comparison

**How?** Fingerprinting is implemented by `Database.get_info` and the *info* step

### ORM

What? interact with a known database specified in JSON configuration

How? This is made possible by `Database.get_model`

### Reverse ORM

**What?** interact with an unknown database using an ORM

**How?** This is made by possible by *introspection* in `Database.get_children`

### Cross-database denormalization

**What?** updating a table with data from another database

**How?** See [this test](tests/integration/test_workflow.py) for an implementation example
