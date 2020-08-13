CONSTRAINT_TYPE_MAP = {
    "exclude": "EXCLUDE",
    "primary": "PRIMARY KEY",
    "foreign": "FOREIGN KEY",
    "unique": "UNIQUE",
    "check": "CHECK",
}


class WithCreateSQL(object):
    def get_column_sql(self, name, column):
        nullable = "NULL" if column.get("null") else "NOT NULL"
        name = self.F.column(name)
        return f'{name} {column["type"]} {nullable}'

    def get_constraint_sql(self, name, constraint):
        check = ""
        if constraint.get("check"):
            check = f' {constraint["check"]} '
            columns = ""
        else:
            columns = constraint["columns"]
            if columns:
                columns = ", ".join([self.F.column(c) for c in columns])
                columns = f" ({columns})"
            else:
                columns = ""

        related_name = constraint.get("related_name")
        related = ""
        if related_name:
            related_columns = ", ".join(
                [self.F.column(c) for c in constraint["related_columns"]]
            )
            related = f" REFERENCES {related_name} ({related_columns})"

        deferrable = constraint.get("deferrable", False)
        deferred = constraint.get("deferred", False)
        deferrable = "DEFERRABLE" if deferrable else "NOT DEFERRABLE"
        deferred = "INITIALLY DEFERRED" if deferred else "INITIALLY IMMEDIATE"
        return (
            f"CONSTRAINT {name} "
            f'{CONSTRAINT_TYPE_MAP[constraint["type"]]}{check}{columns}{related} '
            f"{deferrable} {deferred}"
        )

    def get_create_database_query(self, name):
        database = self.F.database(name)
        return (f"CREATE DATABASE {database}",)

    def get_create_schema_query(self, name):
        schema = self.F.schema(name)
        return (f"CREATE SCHEMA {schema}",)

    def get_create_sequence_query(
        self, name, owned_by=None, temporary=False, maybe=False
    ):
        return {
            'create': {
                'sequence': {
                    'name': name,
                    'owned_by': owned_by,
                    'temporary': temporary,
                    'maybe': maybe
                }
            }
        }

    def get_create_index_query(self, table, name, index, schema=None):
        unique = " UNIQUE" if index["unique"] else ""
        columns = ", ".join([self.F.column(c) for c in index["columns"]])
        type = index["type"]
        table = self.F.table(table, schema=schema)
        return (f"CREATE{unique} INDEX {name} ON {table} USING {type} ({columns})",)

    def get_create_constraint_query(self, table, name, constraint, schema=None):
        constraint = self.get_constraint_sql(name, constraint)
        table = self.F.table(table, schema=schema)
        return (f"ALTER TABLE {table}\nADD {constraint}",)

    def get_create_column_query(self, table, name, column, schema=None):
        column = self.get_column_sql(name, column)
        table = self.F.table(table, schema=schema)
        return (f"ALTER TABLE {table}\nADD COLUMN {column}",)

    def get_create_table_columns_sql(self, table, spaces=2):
        columns = table.get("columns", {})

        if not columns:
            return ""

        sqls = []
        for name, column in columns.items():
            sqls.append(self.get_column_sql(name, column))

        spaces = " " * spaces
        sqls = f",\n{spaces}".join(sqls)
        return f"{spaces}{sqls}"

    def get_create_table_constraints_sql(self, table, spaces=2):
        constraints = table.get("constraints", {})

        if not constraints:
            return ""

        sqls = []
        for name, constraint in constraints.items():
            sqls.append(self.get_constraint_sql(name, constraint))

        spaces = " " * spaces
        constraints = f",\n{spaces}".join(sqls)
        return f"{spaces}{constraints}"

    def get_create_table_query(self, name, table, temporary=False, schema=None):
        columns = self.get_create_table_columns_sql(table)
        constraints = self.get_create_table_constraints_sql(table)
        sep = ""
        if constraints and columns:
            sep = ",\n"

        temp = " TEMPORARY " if temporary else " "
        table = self.F.table(name, schema=schema)
        return (f"CREATE{temp}TABLE {table} (\n{columns}{sep}{constraints}\n)",)

    def get_create_table_indexes_query(self, name, table, schema=None):
        indexes = table.get("indexes", {})
        statements = []
        for index_name, index in indexes.items():
            if index["primary"] or index["unique"]:
                # automatically created by constraints
                continue
            query = self.get_create_index_query(name, index_name, index, schema=schema)
            statements.append(query[0])
        return (";\n".join(statements),) if statements else []


class WithCreate(WithCreateSQL):
    async def create_column(self, table, name, column, schema=None):
        await self.execute(
            *self.get_create_column_query(table, name, column, schema=schema)
        )
        return True

    async def create_columns(self, columns, parents=None):
        return await self.create_table_items("column", columns, parents)

    async def create_constraint(self, table, name, constraint, schema=None):
        await self.execute(
            *self.get_create_constraint_query(table, name, constraint, schema=schema)
        )
        return True

    async def create_constraints(self, constraints, parents=None):
        return await self.create_table_items("constraint", constraints, parents)

    async def create_index(self, table, name, index, schema=None):
        await self.execute(
            *self.get_create_index_query(table, name, index, schema=schema)
        )
        return True

    async def create_indexes(self, indexes, parents=None):
        # ignore indexes with constraints
        # TODO: better way to identify all of these cases:
        # index + constraint: (u)nique, (p)rimary, (f)oreign, e(x)clusion
        # index-only: (c)heck
        return await self.create_table_items(
            "index",
            indexes,
            parents,
            exclude=lambda index: index["primary"] or index['unique']
        )

    async def create_table_items(self, name, data, parents, exclude=None):
        assert len(parents) == 2
        schema, table = parents

        for item_name, item in data.items():
            if exclude and exclude(item):
                continue
            await getattr(self, f"create_{name}")(table, item_name, item, schema=schema)
        return data

    async def create_table(self, name, table, schema=None, temporary=False):
        await self.execute(
            *self.get_create_table_query(
                name, table, schema=schema, temporary=temporary
            )
        )
        query = self.get_create_table_indexes_query(name, table, schema=schema)
        if query:
            await self.execute(*query)
        return True

    async def create_tables(self, tables, parents=None):
        assert len(parents) == 1
        schema = parents[0]

        for table_name, table in tables.items():
            await self.create_table(table_name, table, schema=schema)
        return tables

    async def create_database(self, name):
        await self.execute(*self.get_create_database_query(name))
        return True

    async def create_schema(self, schema):
        await self.execute(*self.get_create_schema_query(schema))
        return True

    async def create_schemas(self, schemas, parents=None):
        for schema, tables in schemas.items():
            await self.create_schema(schema)
            await self.create_tables(tables, parents=[schema])
        return schemas
