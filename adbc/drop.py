class WithDropSQL(object):
    def get_drop_schema_query(self, schema):
        return (f'DROP SCHEMA "{schema}" CASCADE',)

    def get_drop_table_query(self, schema, table):
        return (f'DROP TABLE "{schema}"."{table}" CASCADE',)

    def get_drop_column_query(self, schema, table, name):
        return (f'ALTER TABLE "{schema}"."{table}"\nDROP COLUMN "{name}" CASCADE',)

    def get_drop_constraint_query(self, schema, table, name):
        return (f'ALTER TABLE "{schema}"."{table}"\nDROP CONSTRAINT "{name}" CASCADE',)

    def get_drop_index_query(self, schema, name):
        return (f'DROP INDEX "{schema}"."{name}" CASCADE',)


class WithDrop(WithDropSQL):
    async def drop_column(self, schema, table, name):
        query = self.get_drop_column_query(schema, table, name)
        await self.execute(*query)
        return True

    async def drop_columns(self, columns, parents=None):
        assert len(parents) == 2
        schema, table = parents

        for name in columns.keys():
            await self.drop_column(schema, table, name)
        return columns

    async def drop_constraint(self, schema, table, name):
        query = self.get_drop_constraint_query(schema, table, name)
        await self.execute(*query)
        return True

    async def drop_constraints(self, data, parents=None):
        assert len(parents) == 2
        schema, table = parents

        for name in data.keys():
            await self.drop_constraint(schema, table, name)

        return data

    async def drop_index(self, schema, name):
        if not self._drop_indexes:
            return False

        query = self.get_drop_index_query(schema, name)
        await self.execute(*query)
        return True

    async def drop_indexes(self, data, parents=None):
        assert len(parents) == 2
        schema, table = parents

        for name in data.keys():
            await self.drop_index(schema, name)

        return data

    async def drop_table(self, schema, name):
        await self.execute(*self.get_drop_table_query(schema, name))
        return True

    async def drop_schema(self, schema):
        await self.execute(*self.drop_schema_query(schema))
        return True

    async def drop_tables(self, tables, parents=None):
        assert len(parents) == 1
        schema = parents[0]

        for table in tables.keys():
            await self.drop_table(schema, table)
        return tables

    async def drop_schemas(self, schemas, parents=None):
        for schema_name in schemas.keys():
            await self.drop_schema(schema_name)
        return schemas


