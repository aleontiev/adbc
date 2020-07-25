from adbc.preql.dialect import ParameterStyle, get_default_style
import re
from typing import List, Union, Option
from .core import Builder


CONSTRAINT_TYPE_MAP = {
    "x": "EXCLUDE",
    "p": "PRIMARY KEY",
    "f": "FOREIGN KEY",
    "u": "UNIQUE",
    "c": "CHECK",
}

class SQLBuilder(Builder):
    FORMAT_IDENTIFIER_QUOTE_CHARACTER = '"'

    def get_default_style(self):
        return ParameterStyle.FORMAT

    def add_parameter(self, value, style: ParameterStyle, params: Union[list, dict]):
        num = len(params)
        if style == ParameterStyle.NAMED:
            label = f"p{num}"
            params[param] = value
            return f":{label}"
        elif style == ParameterStyle.NUMERIC:
            params.append(value)
            return f":{num}"
        elif style == ParameterStyle.DOLLAR_NUMERIC:
            params.append(value)
            return f"${num}"
        elif style == ParameterStyle.QUESTION_MARK:
            params.append(value)
            return f"?"
        elif style == ParameterStyle.FORMAT:
            params.append(value)
            return f"%s"
        else:
            raise NotImplementedError(f"unknown style {style}")

    def get_parameters(self, style, params=None):
        return self.get_empty_parameters(style) if params is None else params

    def get_empty_parameters(self, style):
        return {} if style in {ParameterStyle.NAMED} else []

    def build(
        self, query: dict, style: ParameterStyle = None, depth: int = 0, params=None
    ) -> List[tuple]:
        if style is None:
            style = self.get_default_style()

        params = self.get_parameters(style, params)
        keys = list(query.keys())
        if len(keys) != 1:
            raise ValueError("query root must have one key (command name)")

        command_name = keys[0]
        arguments = query[command_name]
        build_method = f"build_{command_name}"
        return getattr(self, build_method)(
            arguments, style=style, depth=depth, params=params
        )

    def build_show(
        self,
        clause: Union[List, dict, str],
        style: ParameterStyle,
        depth: int = 0,
        params=None,
    ) -> List[tuple]:
        raise NotImplementedError()

    def build_drop(
        self,
        clause: Union[List, dict, str],
        style: ParameterStyle,
        depth: int = 0,
        params=None,
    ) -> List[tuple]:
        raise NotImplementedError()

    def build_create(
        self,
        clause: Union[List, dict],
        style: ParameterStyle,
        depth: int = 0,
        params=None,
    ) -> List[tuple]:
        """Builds $.create: create schematic elements

        Can create one or more of the following:
        - database (CREATE DATABASE)
        - schema (CREATE SCHEMA)
        - table (CREATE TABLE)
        - column (ALTER TABLE ADD COLUMN)
        - constraint (ALTER TABLE ADD CONSTRAINT)
        - sequence (CREATE SEQUENCE)
        - index (CREATE INDEX)
        """
        if isinstance(clause, list):
            results = []
            for c in clause:
                results.extend(self.build_create(c, style, depth=depth, params=params))
            return results

        children = (
            "database",
            "schema",
            "table",
            "column",
            "constraint",
            "sequence",
            "index",
        )
        method = None
        for child in children:
            if child in clause:
                method = f"build_create_{child}"
                break

        if method:
            return getattr(self, method)(
                clause[child], style, depth=depth, params=params
            )
        else:
            raise NotImplementedError(f"create expecting to contain one of: {children}")

    def build_alter(self, clause: dict, style: ParameterStyle) -> List[tuple]:
        raise NotImplementedError()

    def build_show(self, clause: dict, style: ParameterStyle) -> List[tuple]:
        raise NotImplementedError()

    def build_update(self, clause: dict, style: ParameterStyle) -> List[tuple]:
        raise NotImplementedError()

    def build_select(self, clause: dict, style: ParameterStyle) -> List[tuple]:
        raise NotImplementedError()

    def build_delete(self, clause: dict, style: ParameterStyle) -> List[tuple]:
        raise NotImplementedError()

    def escape_identifier(self, identifier: Union[list, str]):
        """Escape one or more identifiers"""
        quote = self.FORMAT_IDENTIFIER_QUOTE_CHARACTER
        if isinstance(identifier, list):
            identifier = [identifier]
            return [self.escape_identifier(ident) for ident in identifier]

        if quote not in identifier:
            return identifier

        # escape by doubling quote character
        # TODO: make this configurable?
        identifier = re.sub(quote, f"{quote}{quote}", identifier)
        return identifier

    def format_identifier(self, identifier: Union[list, str]):
        identifier = self.escape_identifier(identifier)
        quote = self.FORMAT_IDENTIFIER_QUOTE_CHARACTER
        if not isinstance(identifier, list):
            if "." in identifier:
                identifier = identifier.split(".")
            else:
                identifier = [identifier]

        return ".".join([f"{quote}{ident}{quote}" for ident in identifier])

    def build_create_database(
        self, clause: str, style: ParameterStyle, depth: int = 0, params=None
    ) -> List[tuple]:
        """Builds $.create.database"""
        indent = " " * self.INDENT * depth
        database = self.format_identifier(clause)
        query = "{indent}CREATE DATABASE {database}"
        return [(query, params)]

    def build_create_schema(
        self, clause: str, style: ParameterStyle, depth: int = 0, params=None
    ) -> List[tuple]:
        """Builds $.create.schema"""
        indent = " " * self.INDENT * depth
        schema = self.format_identifier(clause)
        query = "{indent}CREATE SCHEMA {schema}"
        return [(query, params)]

    def build_create_table(
        self,
        clause: Union[list, str, dict],
        style: ParameterStyle,
        depth: int = 0,
        params=None,
    ) -> List[tuple]:
        """Builds $.create.table"""
        if isinstance(clause, str):
            # name only, no columns or source
            name = self.format_identifier(clause)
            as_ = None
            columns = None
            constraints = None
            temporary = False
            if_not_exists = False
        elif isinstance(clause, list):
            # multiple tables
            results = []
            for c in clause:
                results.extend(
                    self.build_create_table(c, style=style, depth=depth, params=params)
                )
            return results
        else:
            # object
            name = self.format_identifier(command["name"])
            columns = command.get("columns", None)
            constraints = command.get("constraints", None)
            as_ = command.get("as", None)
            temporary = command.get("temporary", False)
            if_not_exists = command.get("if_not_exists", False)

        params = self.get_parameters(style, params)
        temporary = " TEMPORARY " if temporary else " "
        if_not_exists = " IF NOT EXISTS " if if_not_exists else " "
        if as_:
            # CREATE TABLE name AS (SELECT ...)
            subquery = self.build(as_, style, depth=depth + 1, params=params)
            if len(subquery) != 1:
                # expecting subquery to build to exactly one query for this to work
                raise ValueError(f"create table: invalid subquery {as_}")
            subquery, params = subquery[0]
            return [
                (f"CREATE{temporary}TABLE{if_not_exists}{name} AS ({subquery})", params)
            ]
        else:
            # CREATE TABLE name
            if not columns:
                return [(f"CREATE{temporary}TABLE{if_not_exists}{name}", None)]
            # CREATE TABLE name (...columns, constraints...)
            items = self.get_create_table_items(
                columns, style, params, constraints=constraints, depth=depth + 1
            )
            return [
                (f"CREATE{temporary}TABLE{if_not_exists}{name} (\n{items}\n)", params,)
            ]

    def is_command(self, name):
        return name in self.COMMANDS

    def get_operator(self, name):
        return self.OPERATORS.get(name, None)

    def validate_function(self, name):
        # only alphanumeric, _, and .
        # first letter must be alpha
        if re.match(r"^[A-Za-z][A-Za-z0-9_.]*$", name):
            return True
        return False

    def validate_keyword(self, name):
        if re.match(r"^[A-Za-z][A-Za-z_]*$", name):
            return True
        return False

    def format_expression(
        self,
        expression,
        style: ParameterStyle,
        params: Union[dict, list],
        allow_subquery: bool = True,
        depth: int = 0,
    ) -> str:
        if isinstance(expression, (int, float, bool)):
            # literals and identifiers
            return expression

        if expression is None:
            return "NULL"

        if isinstance(expression, list):
            # assume identifier list
            return self.format_identifier(expression)

        if isinstance(expression, str):
            if (
                len(expression) > 1
                and expression[0] == expression[-1]
                and expression[0] in self.QUOTE_CHARACTERS
            ):
                # if the entire string is quoted, assume it is either a literal, identifier, or raw SQL
                # depending on which quote is used
                char = expression[0]
                expression = expression[1:-1]
                if char == self.LITERAL_QUOTE_CHARACTER:
                    return self.add_parameter(expression, style, params)
                elif char == self.IDENTIFIER_QUOTE_CHARACTER:
                    return self.format_identifier(expression)
                elif char == self.RAW_QUOTE_CHARACTER:
                    return expression
            else:
                # if unquoted, always assume an identifier
                return self.format_identifier(expression)

        if isinstance(expression, dict):
            if len(expression.keys()) != 1:
                raise ValueError("object-type expression must have one key")
            key = expression.keys()[0].lower()
            value = expression[key]
            if value is None:
                # keyword expression, e.g. {"default": null} -> DEFAULT (in a VAULES statement)
                if not self.validate_keyword(key):
                    raise ValueError(f'"{key}" is not a valid keyword')
                return key
            else:
                if self.is_command(key):
                    # subquery expression, e.g. {"select": "..."}
                    if not allow_subquery:
                        raise ValueError(
                            f'cannot build "{key}", subqueries not allowed in this expression'
                        )
                    subquery = self.build(
                        value, style=style, depth=depth + 1, params=params
                    )
                    if len(subquery) != 1:
                        raise ValueError(f"expression: invalid subquery {value}")

                    subquery = subquery[0]
                    return subquery[0]

                operator = self.get_operator(key)

                if operator:
                    # operator expression, e.g. {"+": [1, 2]} -> 1 + 2
                    num_args = len(value)
                    if num_args > 1:
                        result = f" {key} ".join(
                            [
                                self.format_expression(
                                    arg,
                                    style,
                                    params,
                                    allow_subquery=allow_subquery,
                                    depth=depth,
                                )
                                for arg in value
                            ]
                        )
                        return f"({result})"
                    else:
                        # e.g. {"not": ...}
                        expression = self.format_expression(
                            value[0],
                            style,
                            params,
                            allow_subquery=allow_subquery,
                            depth=depth,
                        )
                        left = True
                        if (
                            isinstance(operator, dict)
                            and operator.get("binds") == "right"
                        ):
                            # most operators bind left, some bind right
                            left = False
                        return (
                            f"({key} {expression})" if left else f"({expression} {key})"
                        )

                # special cases / unique operators
                if key == "case":
                    raise NotImplementedError("case is not implemented yet")
                if key == "between":
                    val = self.format_expression(
                        value["value"], style, params, allow_subquery, depth
                    )
                    min_value = self.format_expression(
                        value["min"], style, params, allow_subquery, depth
                    )
                    max_value = self.format_expression(
                        value["max"], style, params, allow_subquery, depth
                    )
                    symmetric = value.get("symmetric", False)
                    symmetric = " SYMMETRIC " if symmetric else " "
                    return f"{val} BETWEEN{symmetric}{min_value} AND {max_value}"
                if key == "literal":
                    return self.add_parameter(value, style, params)
                if key == "raw":
                    return value

                # fallback assumption: a function expression, e.g. {"md5": "a"} -> md5("a")
                # user-defined functions can exist
                # functions can be qualified by a schema
                if not self.validate_function(key):
                    raise ValueError(f'"{key}" is not a valid function')
                if not value:
                    arguments = ""
                else:
                    arguments = ", ".join(
                        [
                            self.format_expression(
                                arg,
                                style,
                                params,
                                allow_subquery=allow_subquery,
                                depth=depth,
                            )
                            for arg in value
                        ]
                    )
                return f"{key}({arguments})"

        raise ValueError(f"cannot format expression {expression}")

    def get_create_table_constraint(
        self,
        constraint: dict,
        style: ParameterStyle,
        params: Union[dict, list],
        depth: int = 0,
    ) -> str:
        check = ""
        if constraint.get("check"):
            check = f' {constraint["check"]} '
            columns = ""
        else:
            columns = constraint["columns"]
            if columns:
                columns = ", ".join([self.format_identifier(c) for c in columns])
                columns = f" ({columns})"
            else:
                columns = ""

        related_name = constraint.get("related_name")
        related = ""
        if related_name:
            related_columns = ", ".join(
                [self.format_identifier(c) for c in constraint["related_columns"]]
            )
            related = f" REFERENCES {related_name} ({related_columns})"

        deferrable = constraint.get("deferrable", False)
        deferred = constraint.get("deferred", False)
        deferrable = "DEFERRABLE" if deferrable else "NOT DEFERRABLE"
        deferred = "INITIALLY DEFERRED" if deferred else "INITIALLY IMMEDIATE"
        type = CONSTRAINT_TYPE_MAP[constraint['type']]
        return f"CONSTRAINT {name} {type} {check}{columns}{related} {deferrable} {deferred}"

    def get_create_table_column(
        self,
        column: dict,
        style: ParameterStyle,
        params: Union[dict, list],
        depth: int = 0,
    ) -> str:
        name = column["name"]
        type = column["type"]
        null = column["null"]
        default = column["default"]
        name = self.format_identifier(name)
        null = " NOT NULL " if not null else ""  # null is default
        if default is None:
            default = ""
        else:
            default = self.format_expression(
                default, style, params, allow_subquery=False
            )
            default = f" DEFAULT {default} "
        return f"{name} {type}{null}{default}"

    def get_create_table_items(
        self,
        columns: List[dict],
        style: ParameterStyle,
        params: Union[dict, list],
        constraints: Option[List[dict]] = None,
        depth: int = 0,
    ) -> str:
        """Gets the create table definition body

        This consists of column and constraint "items".
        """
        items = []
        for c in columns:
            items.append(
                self.get_create_table_column(c, style, params, depth=depth + 1)
            )
        if constraints:
            for c in constraints:
                items.append(
                    self.get_create_table_constraint(c, style, params, depth=depth + 1)
                )
        return ",\n".join(items)
