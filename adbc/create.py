CONSTRAINT_TYPE_MAP = {
    "x": "EXCLUDE",
    "p": "PRIMARY KEY",
    "f": "FOREIGN KEY",
    "u": "UNIQUE",
    "c": "CHECK",
}


class WithCreateSQL(object):

    def get_column_sql(self, name, column):
        nullable = "NULL" if column["null"] else "NOT NULL"
        return f'"{name}" {column["type"]} {nullable}'

    def get_constraint_sql(self, name, constraint):
        columns = constraint["columns"]
        if columns:
            columns = ", ".join([f'"{c}"' for c in columns])
            columns = f" ({columns})"
        else:
            columns = ""
        related_name = constraint.get("related_name")
        related = ""
        if related_name:
            related_columns = ",\n  ".join(constraint["related_columns"])
            related = f" REFERENCES {related_name} ({related_columns})"

        deferrable = "DEFERRABLE" if constraint["deferrable"] else "NOT DEFERRABLE"
        deferred = (
            "INITIALLY DEFERRED" if constraint["deferred"] else "INITIALLY IMMEDIATE"
        )
        check = ""
        if constraint["check"]:
            check = f' {constraint["check"]} '
            columns = ""
        return (
            f"CONSTRAINT {name} "
            f'{CONSTRAINT_TYPE_MAP[constraint["type"]]}{check}{columns}{related} '
            f"{deferrable} {deferred}"
        )

    def get_create_schema_query(self, schema):
        return (f'CREATE SCHEMA "{schema}"',)

    def get_create_index_query(self, schema, table, name, index):
        unique = " UNIQUE" if index["unique"] else ""
        columns = ", ".join([f'"{c}"' for c in index["columns"]])
        type = index["type"]
        return (
            f'CREATE{unique} INDEX {name} ON "{schema}"."{table}"\n'
            f"USING {type} ({columns})",
        )

    def get_create_constraint_query(self, schema, table, name, constraint):
        constraint = self.get_constraint_sql(name, constraint)
        return (f'ALTER TABLE "{schema}"."{table}"\n' f"ADD {constraint}",)

    def get_create_column_query(self, schema, table, name, column):
        column = self.get_column_sql(name, column)
        return (f'ALTER TABLE "{schema}"."{table}"\n' f"ADD COLUMN {column}",)

    def get_create_table_columns_sql(self, table, spaces=2):
        table_schema = table.get("schema", {})
        columns = table_schema.get("columns", {})
        if not columns:
            return ""

        sqls = []
        for name, column in columns.items():
            sqls.append(self.get_column_sql(name, column))

        spaces = " " * spaces
        sqls = f",\n{spaces}".join(sqls)
        return f"{spaces}{sqls}"

    def get_create_table_constraints_sql(self, table, spaces=2):
        table_schema = table.get("schema", {})
        constraints = table_schema.get("constraints", {})

        if not constraints:
            return ""

        sqls = []
        for name, constraint in constraints.items():
            sqls.append(self.get_constraint_sql(name, constraint))

        spaces = " " * spaces
        constraints = f",\n{spaces}".join(sqls)
        return f"{spaces}{constraints}"

    def get_create_table_query(self, schema, name, table):
        columns = self.get_create_table_columns_sql(table)
        constraints = self.get_create_table_constraints_sql(table)
        sep = ""
        if constraints and columns:
            sep = ",\n"
        return (
            f'CREATE TABLE "{schema}"."{name}" (\n' f"{columns}{sep}{constraints})",
        )

    def get_create_table_indexes_query(self, schema, table_name, table):
        indexes = table.get("schema", {}).get("indexes", {})
        statements = []
        for name, index in indexes.items():
            if index["primary"] or index["unique"]:
                # automatically created by constraints
                continue
            query = self.get_create_index_query(schema, table_name, name, index)
            statements.append(query[0])
        return (";\n".join(statements),) if statements else []


class WithCreate(object):
    async def create_column(self, schema, table, name, column):
        await self.execute(
            *self.get_create_column_query(schema, table, name, column)
        )
        return True

    async def create_columns(self, columns, parents=None):
        return await self.create_table_items("column", columns, parents)

    async def create_constraint(self, schema, table, name, constraint):
        await self.execute(
            *self.get_create_constraint_query(schema, table, name, constraint)
        )
        return True

    async def create_constraints(self, constraints, parents=None):
        return await self.create_table_items("constraint", constraints, parents)

    async def create_index(self, schema, table, name, index):
        await self.execute(
            *self.get_create_index_query(schema, table, name, index)
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
            exclude=lambda index: index["primary"] or index["unique"],
        )

    async def create_table_items(self, name, data, parents, exclude=None):
        assert len(parents) == 2
        schema, table = parents

        for item_name, item in data.items():
            if exclude and exclude(item):
                continue
            await getattr(self, f"create_{name}")(schema, table, item_name, item)
        return data

    async def create_table(self, schema, name, table):
        await self.execute(*self.get_create_table_query(schema, name, table))
        query = self.get_create_table_indexes_query(schema, name, table)
        if query:
            await self.execute(*query)
        return True

    async def create_tables(self, tables, parents=None):
        assert len(parents) == 1
        schema = parents[0]

        for table_name, table in tables.items():
            await self.create_table(schema, table_name, table)
        return tables

    async def create_schema(self, schema):
        await self.execute(*self.get_create_schema_query(schema))
        return True

    async def create_schemas(self, schemas, parents=None):
        for schema, tables in schemas.items():
            await self.create_schema(schema)
            await self.create_tables(tables, parents=[schema])
        return schemas
