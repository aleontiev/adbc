from adbc.preql.dialect import ParameterStyle, get_default_style
import re
from typing import List, Union, Optional
from .core import Builder


CONSTRAINT_TYPES = {
    "x": "exclude",
    "p": "primary key",
    "f": "foreign key",
    "u": "unique",
    "c": "check",
    "exclude": "exclude",
    "primary": "primary key",
    "foreign": "foreign key",
    "unique": "unique",
    "check": "check"
}
JOIN_TYPES = {
    'inner',
    'left',
    'right',
    'full',
    'cross'
}


class SQLBuilder(Builder):
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

    def build_select(self, clause: Union[dict, list], style: ParameterStyle, depth=0) -> List[tuple]:
        """Builds $.select; read data"""
        if isinstance(clause, list):
            # SELECT ... UNION ALL ... UNION ALL ...
            queries = []
            params = []
            for c in clause:
                query = self.build(c, style, depth=depth+1)
                if len(query) > 1:
                    raise ValueError('select union: expecting one result for each query')

                query, param = query[0]
                queries.append(query)
                if param:
                    params.extend(param)
            return [
                self.combine(queries, separator=' UNION ALL '),
                params
            ]

        params = self.get_empty_parameters()
        results = []
        for child in [
            'with',
            'data',
            'from',
            'join',
            'where',
            'group',
            'having',
            'union',
            'order',
            'limit',
            'offset'
        ]:
            data = clause.get(child)
            results.append(getattr(self, f'get_select_{child}')(data, style, params, depth=depth))

        return self.combine(results, check=True)

    def combine(self, segments, separator='\n', check=False):
        if check:
            return separator.join([s for s in segments if s])
        return separator.join(segments)

    def get_select_with(self, with_, style, params, prefix=True, depth=0) -> str:
        # - with:    dict         ({"as": "...", "query": {"select": {...}})                          # basic subquery
        #                         ({"as": "...", "query": ..., "recursive": True})                    # recursive subquery
        #            list         ([...])                                                             # list of ...
        if prefix:
            prefix = 'WITH '
        else:
            prefix = ''

        if isinstance(with_, list):
            withs = [
                self.get_select_with(w, style, params, prefix=False, depth=depth+1)
                for w in with_
            ]
            return f'{prefix}{withs}'
        if isinstance(with_, dict):
            query = with_.get('query')
            as_ = with_.get('as')
            if not query:
                raise ValueError('select: with must have "query"')
            subquery = self.get_subquery(query, style, params, depth=depth+1)
            as_ = self.format_identifier(as_)
            return f'{prefix}{as_} AS ({query})'

        raise NotImplementedError()

    def get_select_data(self, data, style, params, depth=0) -> str:
        # - data:    string       ("*")                                 # single literal or identifier value
        #            dict         ({"name": "first_name"})              # fully aliased expression list
        #            list[]       ([...])                               # combination of aliased and unaliased values
        if not data:
            raise ValueError('select: must have "data"')

        if isinstance(data, list):
            return self.combine([
                self.get_select_data(d, style, params, depth=depth)
                for d in data
            ])

        if isinstance(data, str):
            return self.get_expression(
                data, style, params, depth=depth, allow_subquery=False
            )

        if isinstance(data, dict):
            for name, value in data.items():
                name = self.format_identifier(name)
                expression = self.get_expression(
                    value, style, params, depth=depth, allow_subquery=True
                )
                results.append(f'{expression} AS {name}')
            return self.combine(results, separator=', ')

        raise NotImplementedError()

    def get_select_from(self, from_, style, params, depth=0):
        # - from:    string       ("users")                  # table by name
        #            dict[string] ({"U": "users"})           # aliased name
        #            dict[dict]   ({"U": {"select": ...}})   # aliased subquery
        #                         ({"U": {"lateral": {...}}  # modifier e.g. LATERAL
        #            list         ([...])                    # list of the above
        if isinstance(from_, string):
            return self.format_identifier(from_)
        if isinstance(from_, dict):
            for name, target in from_.items():
                name = self.format_identifier(name)
                if isinstance(target, str):
                    target = self.format_identifier(target)
                elif isinstance(target, dict):
                    # TODO: support for LATERAL
                    subquery = self.get_subquery(target, style, params, depth=depth+1)
                results.append(f'{target} AS {name}')
                return self.combine(results, separator=', ')
        if isinstance(from_, list):
            return self.combine([
                self.get_select_from(f, style, params, depth=depth)
                for f in from_
            ], separator=', ')

        raise NotImplementedError()

    def get_select_join(self, join, style, params, depth=0) -> str:
        # - join: dict ({"type": "inner", "to": "user", "on": {...}, "as": "u"}} # one join
        #         list ([...])                                                   # list of joins
        if isinstance(join, list):
            return self.combine([
                self.get_select_join(j, style, params, depth=depth)
                for j in join
            ], separator=', ')
        if isinstance(join, dict):
            to = join.get('to')
            is_subquery = False
            if not to:
                raise ValueError('select: join must have "to"')
            if isinstance(to, dict):
                # subquery join
                to = self.get_subquery(to, style, params, depth=depth+1)
                to = f'({to})'
                is_subquery = True
            else:
                # table join
                to = self.format_identifier(to)

            as_ = join.get('as')
            if as_:
                as_ = self.format_identifier(as_)
                as_ = f' AS {as_}'
            else:
                if is_subquery:
                    raise ValueError('select: subquery join must have "as"')
                as_ = ''
            on = join.get('on')
            if on:
                on = self.get_expression(on, style, params, depth=depth)
                on = f' ON {on}'
            else:
                on = ''
            type = join.get('type', 'inner').lower()
            if type not in JOIN_TYPES:
                raise ValueError(f'select: invalid join type "{type}"')
            return f'{type} JOIN {to}{on}{as_}'

    def get_select_where(self, where, style, params, depth=0) -> str:
        # - where:   dict         ({"=": ["id", "1"]})                  # expression
        return self.get_expression(where, style, params, depth=0)

    def get_select_group(self, group, style, params, depth=0) -> str:
        # - group:   string       ("name")                              # simple group by (no rollup)
        #            dict         ({"by": "name", "rollup": True})      # group by condition
        #            list[dict]   ([...])                               # list of conditions
        raise NotImplementedError()

    def get_select_having(self, having, style, params, depth=0) -> str:
        # - having:  dict         ({"!=": ["num_users", 1]})            # an expression
        return self.get_expression(having, style, params, depth=0)

    def get_select_union(self, union, style, params, depth=0) -> str:
        # - union:   dict         ({"select": ...})                     # union query
        #            list[dict]   ([...])
        if isinstance(union, list):
            result = self.combine([
                self.get_select_union(u, style, params, depth=0)
                for u in union
            ], separator=' UNION ')
            return f'UNION {result}'
        if isinstance(union, dict):
            subquery = self.get_subquery(union, style, params, depth=depth+1)
            return f'UNION {subquery}'

        raise NotImplementedError()

    def get_select_order(self, order, style, params, depth=0, prefix=True) -> str:
        # - order:   string       ("name")                              # simple order by (ascending)
        #            dict         ({"by": "name", "asecending": True})  # order by condition
        #            list[dict]   ([...])                               # list thereof
        if prefix:
            prefix = 'ORDER BY '
        else:
            prefix = ''

        if isinstance(order, list):
            order = self.combine([
                self.get_select_order(o, style, params, depth=depth, prefix=False)
                for o in order
            ], separator=', ')
            return f'{prefix}{order}'
        if isinstance(order, str):
            order = self.format_identifier(order)
            return f'{prefix}{order} ASC'
        if isinstance(order, dict):
            by = order.get('by')
            if not by:
                raise ValueError('select: order object must have "by"')
            ascending = order.get('ascending', True)
            # default order always ascending
            direction = '' if ascending else ' DESC'
            by = self.format_identifier(by)
            return f'{prefix}{by}{direction}'

    def get_select_limit(self, limit, style, params) -> str:
        # - limit:   integer      (1)                                   # an integer
        if limit is None:
            return None
        return str(int(limit))

    def get_select_offset(self, union, style, params) -> str:
        # - offset:  integer      (1)                                   # an integer
        if offset is None:
            return None
        return str(int(offset))

    def build_delete(self, clause: dict, style: ParameterStyle) -> List[tuple]:
        raise NotImplementedError()

    def escape_identifier(self, identifier: Union[list, str]):
        """Escape one or more identifiers"""
        if isinstance(identifier, list):
            return identifier

        quote = self.IDENTIFIER_QUOTE_CHARACTER
        if quote not in identifier:
            return identifier

        # escape by doubling quote character
        # TODO: make this configurable?
        identifier = re.sub(quote, f"{quote}{quote}", identifier)
        return identifier

    def unpack_identifier(self, identifier: Union[list, str, dict]):
        split_on = self.IDENTIFIER_SPLIT_CHARACTER

        if isinstance(identifier, dict):
            if "identifier" not in identifier:
                raise ValueError('expecting object identifier to have "identifier" key')
            identifier = identifier["identifier"]

        identifier = self.escape_identifier(identifier)
        if not isinstance(identifier, list):
            if split_on in identifier:
                identifier = identifier.split(split_on)
            else:
                identifier = [identifier]
        return identifier

    def format_identifier(self, identifier: Union[list, str, dict]):
        identifier = self.unpack_identifier(identifier)
        quote = self.IDENTIFIER_QUOTE_CHARACTER
        return self.combine([f"{quote}{ident}{quote}" for ident in identifier], separator=".")

    def build_create_database(
        self, clause: str, style: ParameterStyle, depth: int = 0, params=None
    ) -> List[tuple]:
        """Builds $.create.database"""
        indent = " " * self.INDENT * depth
        database = self.format_identifier(clause)
        query = f"{indent}CREATE DATABASE {database}"
        return [(query, params)]

    def build_create_schema(
        self, clause: str, style: ParameterStyle, depth: int = 0, params=None
    ) -> List[tuple]:
        """Builds $.create.schema"""
        indent = " " * self.INDENT * depth
        schema = self.format_identifier(clause)
        query = f"{indent}CREATE SCHEMA {schema}"
        return [(query, params)]

    def build_create_table(
        self,
        clause: Union[list, str, dict],
        style: ParameterStyle,
        depth: int = 0,
        params=None,
    ) -> List[tuple]:
        """Builds $.create.table"""

        indent = " " * self.INDENT * depth
        if isinstance(clause, str):
            # name only, no columns or source
            name = clause
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
            name = clause['name']
            columns = clause.get("columns", None)
            constraints = clause.get("constraints", None)
            as_ = clause.get("as", None)
            temporary = clause.get("temporary", False)
            if_not_exists = clause.get("maybe", False)

        params = self.get_parameters(style, params)
        temporary = " TEMPORARY " if temporary else " "
        if_not_exists = " IF NOT EXISTS " if if_not_exists else " "
        name = self.format_identifier(name)
        if as_:
            # CREATE TABLE name AS (SELECT ...)
            subquery = self.get_subquery(as_, style, params, depth=depth+1)
            return [
                (
                    f"{indent}CREATE{temporary}TABLE{if_not_exists}{name} AS ({subquery})",
                    params,
                )
            ]
        else:
            # CREATE TABLE name
            if not columns:
                return [(f"{indent}CREATE{temporary}TABLE{if_not_exists}{name}", None)]
            # CREATE TABLE name (...columns, constraints...)
            items = self.get_create_table_items(
                columns, style, params, constraints=constraints, depth=depth + 1
            )
            return [
                (
                    f"{indent}CREATE{temporary}TABLE{if_not_exists}{name} (\n{items}\n)",
                    params,
                )
            ]

    def get_subquery(self, data, style, params, depth=0) -> str:
        result = self.build(as_, style, depth=depth + 1, params=params)
        if len(result) != 1:
            # expecting subquery to build to exactly one query for this to work
            raise ValueError(f'subquery: expecting one query')
        return result[0]

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

    def get_expression(
        self,
        expression,
        style: ParameterStyle,
        params: Union[dict, list],
        allow_subquery: bool = True,
        depth: int = 0,
    ) -> str:
        if isinstance(expression, (int, float, bool)):
            # literal
            return expression

        if expression is None:
            return "NULL"

        if isinstance(expression, list):
            # assume identifier list
            return self.format_identifier(expression)

        if isinstance(expression, str):
            if expression == self.WILDCARD_CHARACTER:
                return expression
            if (
                len(expression) > 1
                and expression[0] == expression[-1]
                and expression[0] in self.QUOTE_CHARACTERS
            ):
                # if quotes with ', ", or `, assume this is a literal
                char = expression[0]
                expression = expression[1:-1]
                return self.add_parameter(expression, style, params)
            else:
                # if unquoted, always assume an identifier
                return self.format_identifier(expression)

        if isinstance(expression, dict):
            keys = list(expression.keys())
            if len(keys) != 1:
                raise ValueError("object-type expression must have one key")
            key = keys[0].lower()
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
                        result = self.combine(
                            [
                                self.get_expression(
                                    arg,
                                    style,
                                    params,
                                    allow_subquery=allow_subquery,
                                    depth=depth,
                                )
                                for arg in value
                            ],
                            separator=f' {key} '
                        )
                        return f"({result})"
                    else:
                        # e.g. {"not": ...}
                        expression = self.get_expression(
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

                # special cases
                if key == "case":
                    raise NotImplementedError("case is not implemented yet")
                if key == "between":
                    val = self.get_expression(
                        value["value"], style, params, allow_subquery, depth
                    )
                    min_value = self.get_expression(
                        value["min"], style, params, allow_subquery, depth
                    )
                    max_value = self.get_expression(
                        value["max"], style, params, allow_subquery, depth
                    )
                    symmetric = value.get("symmetric", False)
                    symmetric = " SYMMETRIC " if symmetric else " "
                    return f"{val} BETWEEN{symmetric}{min_value} AND {max_value}"
                if key == "identifier":
                    return self.format_identifier(value)
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
                    arguments = self.combine(
                        [
                            self.get_expression(
                                arg,
                                style,
                                params,
                                allow_subquery=allow_subquery,
                                depth=depth,
                            )
                            for arg in value
                        ],
                        separator=', '
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
        name = constraint.get("name")
        type = constraint.get("type")
        indent = " " * self.INDENT * depth

        if not name:
            raise ValueError('constraint must have "name"')
        if not type:
            raise ValueError(f'constraint "{name}" must have key: "type"')

        if type not in CONSTRAINT_TYPES:
            raise ValueError(f'constraint "{name}" has invalid type: "{type}"')

        type = CONSTRAINT_TYPES[type]
        check = ""
        if constraint.get("check"):
            # check constraints use an expression
            check = constraint['check']
            check = self.get_expression(check, style, params, depth=depth)
            check = f" {check}"
            columns = ""
        else:
            # non-check constraints must have columns
            if "columns" not in constraint:
                raise ValueError(f'{type} constraint: "{name}" must have key: "columns"')
            columns = constraint["columns"]
            columns = self.combine([self.format_identifier(c) for c in columns], separator=', ')
            columns = f" ({columns})"

        related = ""
        if type == "foreign key":
            related_name = constraint.get("related_name")
            related_columns = constraint.get("related_columns")
            if not related_name or not related_columns:
                raise ValueError(
                    f'{type} constraint must have "related_columns" and "related_name"'
                )

            related_name = self.format_identifier(related_name)
            related_columns = self.combine(
                [self.format_identifier(c) for c in related_columns],
                separator=', '
            )
            related = f" REFERENCES {related_name} ({related_columns})"

        deferrable = constraint.get("deferrable", False)
        deferred = constraint.get("deferred", False)
        deferrable = "DEFERRABLE" if deferrable else "NOT DEFERRABLE"
        deferred = "INITIALLY DEFERRED" if deferred else "INITIALLY IMMEDIATE"
        name = self.format_identifier(name)
        type = type.upper()  # costmetic
        return f"{indent}CONSTRAINT {name} {type}{check}{columns}{related} {deferrable} {deferred}"

    def get_create_table_column(
        self,
        column: dict,
        style: ParameterStyle,
        params: Union[dict, list],
        depth: int = 0,
    ) -> str:
        # name and type are required
        indent = " " * self.INDENT * depth
        name = column.get("name")
        type = column.get("type")
        if not name:
            raise ValueError("column must have name")
        if not type:
            raise ValueError(f'column "{name}" must have type')

        # null, default are optional
        null = column.get("null", True)
        default = column.get("default", None)
        null = " NOT NULL" if not null else ""  # null is default
        if default is None:
            default = ""
        else:
            default = self.get_expression(
                default, style, params, allow_subquery=False
            )
            default = f" DEFAULT {default}"

        name = self.format_identifier(name)
        return f"{indent}{name} {type}{null}{default}"

    def get_create_table_items(
        self,
        columns: List[dict],
        style: ParameterStyle,
        params: Union[dict, list],
        constraints: Optional[List[dict]] = None,
        depth: int = 0,
    ) -> str:
        """Gets the create table definition body

        This consists of column and constraint "items".
        """
        items = []
        for c in columns:
            items.append(
                self.get_create_table_column(c, style, params, depth=depth)
            )
        if constraints:
            for c in constraints:
                items.append(
                    self.get_create_table_constraint(c, style, params, depth=depth)
                )
        return self.combine(items, separator=',\n')
