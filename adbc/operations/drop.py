class WithDropPreQL(object):
    def get_drop_database_query(self, name):
        return {'drop': {'database': name}}

    def get_drop_schema_query(self, name):
        return {'drop': {'schema': name, 'cascade': True}}

    def get_drop_table_query(self, table, schema=None):
        table = f'{schema}.{table}' if schema else table
        return {'drop': {'table': {'name': table, 'cascade': True}}}

    def get_drop_column_query(self, table, name, schema=None):
        table = f'{schema}.{table}' if schema else table
        return {'drop': {'column': {'on': table, 'name': name}}}

    def get_drop_constraint_query(self, table, name, schema=None):
        table = f'{schema}.{table}' if schema else table
        return {'drop': {'constraint': {'on': table, 'name': name}}}

    def get_drop_index_query(self, name, schema=None):
        index = f'{schema}.{index}' if schema else index
        return {'drop': {'index': {'name': index, 'cascade': True}}}


class WithDrop(WithDropPreQL):
    async def drop_column(self, table, name, schema=None):
        query = self.get_drop_column_query(table, name, schema=schema)
        await self.execute(query)
        return True

    async def drop_columns(self, columns, parents=None):
        assert len(parents) == 2
        schema, table = parents

        for name in columns.keys():
            await self.drop_column(table, name, schema=None)
        return columns

    async def drop_constraint(self, table, name, schema=None):
        query = self.get_drop_constraint_query(table, name, schema=schema)
        await self.execute(query)
        return True

    async def drop_constraints(self, data, parents=None):
        assert len(parents) == 2
        schema, table = parents

        for name in data.keys():
            await self.drop_constraint(table, name, schema=schema)

        return data

    async def drop_index(self, name, schema=None):
        if not self._drop_indexes:
            return False

        query = self.get_drop_index_query(name, schema=None)
        await self.execute(query)
        return True

    async def drop_indexes(self, data, parents=None):
        assert len(parents) == 2
        schema, table = parents

        for name in data.keys():
            await self.drop_index(name, schema=None)

        return data

    async def drop_table(self, name, schema=None):
        await self.execute(self.get_drop_table_query(name, schema=schema))
        return True

    async def drop_schema(self, schema):
        await self.execute(self.get_drop_schema_query(schema))
        return True

    async def drop_database(self, name):
        await self.execute(self.get_drop_database_query(name))
        return True

    async def drop_tables(self, tables, parents=None):
        assert len(parents) == 1
        schema = parents[0]

        for table in tables.keys():
            await self.drop_table(table, schema=schema)
        return tables

    async def drop_schemas(self, schemas, parents=None):
        for name in schemas.keys():
            await self.drop_schema(name)
        return schemas
