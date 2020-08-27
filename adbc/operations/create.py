import copy


class WithCreateQuery(object):
    def get_create_database_query(self, name):
        return {
            'create': {
                'database': name
            }
        }

    def get_create_schema_query(self, name):
        return {
            'create': {
                'schema': name
            }
        }

    def get_create_sequence_query(
        self,
        name,
        owned_by=None,
        temporary=False,
        maybe=False
    ):
        return {
            "create": {
                "sequence": {
                    "name": name,
                    "owned_by": owned_by,
                    "temporary": temporary,
                    "maybe": maybe,
                }
            }
        }

    def get_create_index_query(self, table, name, index, schema=None):
        if schema:
            table = f'{schema}.{table}'

        index = copy.copy(index)
        index['name'] = name
        index['on'] = table
        return {
            'create': {
                'index': index
            }
        }

    def get_create_constraint_query(self, table, name, constraint, schema=None):
        if schema:
            table = f'{schema}.{table}'

        constraint = copy.copy(constraint)
        constraint['name'] = name
        constraint['on'] = table
        return {
            'create': {
                'constraint': constraint
            }
        }

    def get_create_column_query(self, table, name, column, schema=None):
        if schema:
            table = f'{schema}.{table}'

        column = copy.copy(column)
        column['name'] = name
        column['on'] = table
        return {
            'create': {
                'column': column
            }
        }

    def get_create_table_query(
        self, name, table, maybe=False, temporary=False, schema=None
    ):
        if schema:
            name = f"{schema}.{name}"
        return {
            "create": {
                "table": {
                    "name": name,
                    "temporary": temporary,
                    "maybe": maybe,
                    "columns": table.get("columns"),
                    "constraints": table.get("constraints"),
                    "indexes": table.get("indexes"),
                }
            }
        }


class WithCreate(WithCreateQuery):
    async def create_column(self, table, name, column, schema=None):
        return await self.execute(
            self.get_create_column_query(table, name, column, schema=schema)
        )

    async def create_columns(self, columns, parents=None):
        return await self.create_table_items("column", columns, parents)

    async def create_constraint(self, table, name, constraint, schema=None):
        return await self.execute(
            self.get_create_constraint_query(table, name, constraint, schema=schema)
        )

    async def create_constraints(self, constraints, parents=None):
        return await self.create_table_items("constraint", constraints, parents)

    async def create_index(self, table, name, index, schema=None):
        return await self.execute(
            self.get_create_index_query(table, name, index, schema=schema)
        )

    async def create_indexes(self, indexes, parents=None):
        # ignore indexes with constraints
        # TODO: better way to identify all of these cases:
        # index + constraint: (u)nique, (p)rimary, (f)oreign, e(x)clusion
        # index-only: (c)heck
        return await self.create_table_items(
            "index",
            indexes,
            parents,
            exclude=lambda index: index["primary"] or index["unique"],
        )

    async def create_table_items(self, name, data, parents, exclude=None):
        assert len(parents) == 2
        schema, table = parents

        for item_name, item in data.items():
            if exclude and exclude(item):
                continue
            await getattr(self, f"create_{name}")(table, item_name, item, schema=schema)
        return data

    async def create_table(self, name, table, schema=None, temporary=False, maybe=False):
        return await self.execute(
            self.get_create_table_query(
                name,
                table,
                schema=schema,
                temporary=temporary,
                maybe=maybe
            )
        )

    async def create_tables(self, tables, parents=None):
        assert len(parents) == 1
        schema = parents[0]

        for table_name, table in tables.items():
            await self.create_table(table_name, table, schema=schema)
        return tables

    async def create_database(self, name):
        return await self.execute(self.get_create_database_query(name))

    async def create_schema(self, schema):
        return await self.execute(self.get_create_schema_query(schema))

    async def create_schemas(self, schemas, parents=None):
        for schema, tables in schemas.items():
            await self.create_schema(schema)
            await self.create_tables(tables, parents=[schema])
        return schemas
