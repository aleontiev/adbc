from adbc.preql.dialect import ParameterStyle, get_default_style
import re
from collections import defaultdict
from typing import List, Union, Optional
from .core import Builder

# from .statements import Select, ...


CONSTRAINT_TYPES = {
    "x": "exclude",
    "p": "primary key",
    "f": "foreign key",
    "u": "unique",
    "c": "check",
    "exclude": "exclude",
    "primary": "primary key",
    "primary key": "primary key",
    "foreign key": "foreign key",
    "foreign": "foreign key",
    "unique": "unique",
    "check": "check",
}
INDEX_TYPES = {"btree": "btree", "hash": "hash", "gist": "gist", "gin": "gin"}

JOIN_TYPES = {
    "inner": "inner",
    "left": "left",
    "left outer": "left",
    "right": "right",
    "right outer": "right",
    "full": "full",
    "cross": "cross",
}


class SQLBuilder(Builder):
    def get_default_style(self):
        return ParameterStyle.FORMAT

    def add_parameter(
        self, value, style: ParameterStyle, params: Union[list, dict], name=None
    ):
        num = len(params)
        if name and not style == ParameterStyle.NAMED:
            raise ValueError("can only add named parameters with style NAMED")

        if style == ParameterStyle.NAMED:
            label = name if name else f"p{num}"
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

    def extend_parameters(self, params, other):
        if isinstance(params, list):
            return params.extend(other)
        if isinstance(params, dict):
            return params.update(other)
        raise NotImplementedError()

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
        by_table=False,
    ) -> Union[List[tuple], dict]:
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
        indent = " " * self.INDENT * depth
        if isinstance(clause, list):
            all_results = []
            table_results = {}
            for c in clause:
                # try to get results by table
                subresults = self.build_create(
                    c, style, depth=depth, params=params, by_table=True
                )
                if isinstance(subresults, list):
                    all_results.extend(subresults)
                elif isinstance(subresults, dict):
                    # results by tables, only for "column" and "constraint"
                    for key, value in subresults.items():
                        value, ps = value
                        if key not in table_results:
                            # add table
                            table_results[key] = (value, ps)
                        else:
                            # merge table
                            table_results[key][0].extend(value)
                            self.extend_parameters(table_results[key][1], ps)
            if by_table:
                return table_results

            indent2 = " " * self.INDENT * (depth + 1)
            for name, results in table_results.items():
                name = self.format_identifier(name)
                results, params = results
                separator = f"\n{indent2}" if len(results) > 1 else " "
                results = self.combine(results, separator=separator)
                results = f"{indent}ALTER TABLE {name}{separator}{results}"
                all_results.append((results, params))

            return all_results

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
            method = getattr(self, method)
            kwargs = {"depth": depth, "params": params}
            if by_table and child == "column" or child == "constraint":
                kwargs["by_table"] = True
            return method(clause[child], style, **kwargs)
        else:
            raise NotImplementedError(f"create expecting to contain one of: {children}")

    def build_alter(
        self, clause: dict, style: ParameterStyle, params=None, depth: int = 0, by_table=False
    ) -> List[tuple]:
        """Builds $.alter: modifies schematic elements

        Can alter the following:
            database: (ALTER DATABASE)
            schema: (ALTER SCHEMA)
            table: (ALTER TABLE *)
            column (ALTER TABLE ALTER COLUMN)
            constraint (ALTER TABLE ALTER CONSTRAINT)
            sequence (ALTER SEQUENCE)
        """
        indent = " " * self.INDENT * depth
        if isinstance(clause, list):
            all_results = []
            table_results = {}
            for c in clause:
                # try to get results by table
                subresults = self.build_alter(
                    c, style, depth=depth, params=params, by_table=True
                )
                if isinstance(subresults, list):
                    all_results.extend(subresults)
                elif isinstance(subresults, dict):
                    # results by tables, only for "column" and "constraint"
                    for key, value in subresults.items():
                        value, ps = value
                        if key not in table_results:
                            # add table
                            table_results[key] = (value, ps)
                        else:
                            # merge table
                            table_results[key][0].extend(value)
                            self.extend_parameters(table_results[key][1], ps)
            if by_table:
                return table_results

            indent2 = " " * self.INDENT * (depth + 1)
            for name, results in table_results.items():
                name = self.format_identifier(name)
                results, params = results
                separator = f"\n{indent2}" if len(results) > 1 else " "
                results = self.combine(results, separator=separator)
                results = f"{indent}ALTER TABLE {name}{separator}{results}"
                all_results.append((results, params))

            return all_results

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
            method = getattr(self, method)
            kwargs = {"depth": depth, "params": params}
            if by_table and child == "column" or child == "constraint":
                kwargs["by_table"] = True
            return method(clause[child], style, **kwargs)
        else:
            raise NotImplementedError(f"create expecting to contain one of: {children}")

    def build_show(self, clause: dict, style: ParameterStyle) -> List[tuple]:
        raise NotImplementedError()

    def build_update(self, clause: dict, style: ParameterStyle) -> List[tuple]:
        raise NotImplementedError()

    def build_select(
        self, clause: Union[dict, list], style: ParameterStyle, params, depth=0
    ) -> List[tuple]:
        """Builds $.select; read data"""
        if isinstance(clause, list):
            # SELECT ... UNION ALL ... UNION ALL ...
            queries = []
            params = []
            for c in clause:
                param = self.get_parameters(style, params)
                query = self.get_subquery(c, style, param, depth=depth)
                queries.append(query)
                self.extend_parameters(params, param)
            return [self.combine(queries, separator=" UNION ALL "), params]

        params = self.get_parameters(style, params)
        results = []
        for child in [
            "with",
            "data",
            "from",
            "join",
            "where",
            "group",
            "having",
            "union",
            "order",
            "limit",
            "offset",
        ]:
            data = clause.get(child)
            results.append(
                getattr(self, f"get_select_{child}")(data, style, params, depth=depth)
            )

        return [(self.combine(results, check=True), params)]

    def combine(self, segments, separator="\n", check=False):
        if check:
            return separator.join([s for s in segments if s])
        return separator.join(segments)

    def get_select_with(self, with_, style, params, prefix=True, depth=0) -> str:
        # dict ({"as": "...", "query": {"select": {...}})       # basic subquery
        #      ({"as": "...", "query": ..., "recursive": True}) # recursive subquery
        # list ([...])                                          # list of ...
        if prefix:
            prefix = "WITH "
        else:
            prefix = ""

        if not with_:
            return None

        if isinstance(with_, list):
            withs = [
                self.get_select_with(w, style, params, prefix=False, depth=depth + 1)
                for w in with_
            ]
            return f"{prefix}{withs}"

        if isinstance(with_, dict):
            query = with_.get("query")
            as_ = with_.get("as")
            if not query:
                raise ValueError('select: with must have "query"')
            subquery = self.get_subquery(query, style, params, depth=depth)
            as_ = self.format_identifier(as_)
            return f"{prefix}{as_} AS (\n{query}\n)"

        raise NotImplementedError()

    def get_select_data(self, data, style, params, prefix=True, depth=0) -> str:
        # string       ("*")                                 # single literal or identifier value
        # dict         ({"name": "first_name"})              # fully aliased expression list
        # list[]       ([...])                               # combination of aliased and unaliased values
        if not data:
            raise ValueError('select: must have "data"')

        indent = " " * self.INDENT * depth
        if prefix:
            prefix = f"{indent}SELECT\n"
        else:
            prefix = indent

        if isinstance(data, list):
            result = self.combine(
                [
                    self.get_select_data(
                        d,
                        style,
                        params,
                        prefix=False,
                        depth=depth + (0 if isinstance(d, (dict, list)) else 1),
                    )
                    for d in data
                ],
                separator=",\n",
            )
            return f"{prefix}{result}"

        if isinstance(data, str):
            result = self.get_expression(
                data, style, params, indent=False, depth=depth, allow_subquery=False
            )
            return f"{prefix}{result}"

        if isinstance(data, dict):
            results = []
            for name, value in data.items():
                name = self.format_identifier(name)
                expression = self.get_expression(
                    value, style, params, depth=depth + 1, allow_subquery=True
                )
                results.append(f"{expression} AS {name}")
            result = self.combine(results, separator=",\n")
            if prefix == indent:
                prefix = ""
            return f"{prefix}{result}"

        raise NotImplementedError()

    def get_select_from(self, from_, style, params, prefix=True, depth=0):
        # string       ("users")                  # table by name
        # dict[string] ({"u": "users"})           # aliased name
        # dict[dict]   ({"u": {"select": ...}})   # aliased subquery
        #              ({"u": {"lateral": {...}}  # modifier e.g. LATERAL
        # list         ([...])                    # list of the above
        indent = " " * self.INDENT * depth
        if prefix:
            prefix = f"{indent}FROM "
        else:
            prefix = indent

        if not from_:
            return None  # rare, selecting literals or values

        if isinstance(from_, str):
            from_ = self.format_identifier(from_)
            return f"{prefix}{from_}"

        if isinstance(from_, dict):
            for name, target in from_.items():
                name = self.format_identifier(name)
                if isinstance(target, str):
                    target = self.format_identifier(target)
                elif isinstance(target, dict):
                    # TODO: support for LATERAL
                    subquery = self.get_subquery(target, style, params, depth=depth)
                results.append(f"{target} AS {name}")
                result = self.combine(results, separator=", ")
                return f"{prefix}{result}"

        if isinstance(from_, list):
            result = self.combine(
                [
                    self.get_select_from(f, style, params, depth=depth, prefix=False)
                    for f in from_
                ],
                separator=", ",
            )
            return f"{prefix}{result}"

        raise NotImplementedError()

    def get_select_join(self, join, style, params, depth=0) -> str:
        # - join: dict ({"type": "inner", "to": "user", "on": {...}, "as": "u"}} # one join
        #         list ([...])                                                   # list of joins
        indent = " " * self.INDENT * depth

        if not join:
            return None

        if isinstance(join, list):
            return self.combine(
                [self.get_select_join(j, style, params, depth=depth) for j in join],
                separator="\n",
            )
        if isinstance(join, dict):
            to = join.get("to")
            is_subquery = False
            if not to:
                raise ValueError('select: join must have "to"')
            if isinstance(to, dict):
                # subquery join
                to = self.get_subquery(to, style, params, depth=depth)
                to = f"({to})"
                is_subquery = True
            else:
                # table join
                to = self.format_identifier(to)

            as_ = join.get("as")
            if as_:
                as_ = self.format_identifier(as_)
                as_ = f" AS {as_}"
            else:
                if is_subquery:
                    raise ValueError('select: subquery join must have "as"')
                as_ = ""
            on = join.get("on")
            if on:
                on = self.get_expression(on, style, params, indent=False)
                on = f" ON {on}"
            else:
                on = ""
            type = join.get("type", "inner").lower()
            if type not in JOIN_TYPES:
                raise ValueError(f'select: invalid join type "{type}"')
            type = JOIN_TYPES[type].upper()
            return f"{indent}{type} JOIN {to}{as_}{on}"

    def get_select_where(self, where, style, params, depth=0) -> str:
        # dict ({"=": ["id", "1"]}) # expression
        if where is None:
            return None
        indent = " " * self.INDENT * depth
        where = self.get_expression(where, style, params, indent=False, depth=depth)
        return f"{indent}WHERE {where}"

    def get_select_group(self, group, style, params, prefix=True, depth=0) -> str:
        # string       ("name")                         # simple group by (no rollup)
        # dict         ({"by": "name", "rollup": True}) # group by condition
        # list[dict]   ([...])                          # list of conditions
        indent = " " * self.INDENT * depth
        if prefix:
            prefix = f"{indent}GROUP BY "
        else:
            prefix = f"{indent}"

        if isinstance(group, list):
            group = self.combine(
                [
                    self.get_select_group(g, style, params, depth=depth, prefix=False)
                    for g in group
                ],
                separator=", ",
            )
            return f"{prefix}{group}"
        if isinstance(group, str):
            order = self.format_identifier(group)
            return f"{prefix}{group}"
        if isinstance(group, dict):
            by = group.get("by")
            if not by:
                raise ValueError('group: group object must have "by"')
            rollup = group.get("rollup", False)
            # default no rollup
            rollup = " WITH ROLLUP" if rollup else ""
            by = self.format_identifier(by)
            return f"{prefix}{by}{rollup}"

    def get_select_having(self, having, style, params, depth=0) -> str:
        # - having:  dict         ({"!=": ["num_users", 1]})            # an expression
        if having is None:
            return None
        indent = " " * self.INDENT * depth
        having = self.get_expression(having, style, params, indent=False, depth=depth)
        return f"{indent}HAVING {having}"

    def get_select_union(self, union, style, params, depth=0) -> str:
        # - union:   dict         ({"select": ...})                     # union query
        #            list[dict]   ([...])
        if not union:
            return None

        indent = " " * self.INDENT * depth
        if isinstance(union, list):
            result = self.combine(
                [self.get_select_union(u, style, params, depth=depth) for u in union],
                separator=" UNION ",
            )
            return f"{indent}UNION {result}"
        if isinstance(union, dict):
            subquery = self.get_subquery(union, style, params, depth=depth)
            return f"{indent}UNION {subquery}"

        raise NotImplementedError()

    def get_select_order(self, order, style, params, depth=0, prefix=True) -> str:
        # - order:   string       ("name")                              # simple order by (ascending)
        #            dict         ({"by": "name", "asecending": True})  # order by condition
        #            list[dict]   ([...])                               # list thereof
        indent = " " * self.INDENT * depth
        if prefix:
            prefix = f"{indent}ORDER BY "
        else:
            prefix = indent

        if isinstance(order, list):
            order = self.combine(
                [
                    self.get_select_order(o, style, params, depth=depth, prefix=False)
                    for o in order
                ],
                separator=", ",
            )
            return f"{prefix}{order}"
        if isinstance(order, str):
            order = self.format_identifier(order)
            return f"{prefix}{order} ASC"
        if isinstance(order, dict):
            by = order.get("by")
            if not by:
                raise ValueError('select: order object must have "by"')
            ascending = order.get("ascending", True)
            # default order always ascending
            direction = "" if ascending else " DESC"
            by = self.format_identifier(by)
            return f"{prefix}{by}{direction}"

    def get_select_limit(self, limit, style, params, depth=0) -> str:
        # - limit:   integer      (1)                                   # an integer
        if limit is None:
            return None
        indent = " " * self.INDENT * depth
        limit = int(limit)
        return f"{indent}LIMIT {limit}"

    def get_select_offset(self, offset, style, params, depth=0) -> str:
        # - offset:  integer      (1)                                   # an integer
        if offset is None:
            return None
        indent = " " * self.INDENT * depth
        offset = int(offset)
        return f"{indent}OFFSET {offset}"

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
        return self.combine(
            [f"{quote}{ident}{quote}" for ident in identifier], separator="."
        )

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

    def build_create_sequence(
        self,
        clause: Union[str, dict],
        style: ParameterStyle,
        depth: int = 0,
        params=None,
    ):
        """Builds $.create.sequence"""
        indent = " " * self.INDENT * depth
        if isinstance(clause, str):
            name = clause
            temporary = False
            maybe = False
            min_value = None
            max_value = None
            start = None
            owned_by = None
            increment = None
        else:
            name = clause["name"]
            temporary = clause.get("temporary", False)
            maybe = clause.get("maybe", False)
            min_value = clause.get("min_value")
            if min_value:
                min_value = int(min_value)
            max_value = clause.get("max_value")
            if max_value:
                max_value = int(max_value)
            start = clause.get("start")
            if start:
                start = int(start)
            increment = clause.get("increment")
            if increment:
                increment = int(increment)
            owned_by = clause.get("owned_by")
            if owned_by:
                owned_by = self.format_identifier(owned_by)

        name = self.format_identifier(name)
        temporary = " TEMPORARY " if temporary else " "
        maybe = " IF NOT EXISTS " if maybe else " "
        owned_by = f" OWNED BY {owned_by}"
        min_value = f" MINVALUE {min_value}" if min_value else ""
        max_value = f" MAXVALUE {max_value}" if max_value else ""
        start = f" START WITH {start}" if start else ""
        increment = f" INCREMENT BY {increment}" if increment else ""
        query = (
            f"{indent}CREATE{temporary}SEQUENCE{maybe}{name}"
            f"{increment}{min_value}{max_value}{start}{owned_by}"
        )
        return [(query, params)]

    def build_create_column(
        self,
        clause: Union[list, dict],
        style: ParameterStyle,
        depth: int = 0,
        params=None,
        by_table=False,
    ):
        return self.build_create_table_item(
            "column", clause, style, depth=depth, params=params, by_table=by_table,
        )

    def build_create_table_item(
        self,
        type,
        clause: Union[list, dict],
        style: ParameterStyle,
        depth: int = 0,
        params=None,
        by_table=False,
    ):
        """Builds $.create.{column, constraint}"""
        if type == "column":
            # optional prefix for column definitions
            prefix = f"COLUMN "
        else:
            prefix = ""

        indent = " " * self.INDENT * depth
        indent2 = " " * self.INDENT * (depth + 1)
        if by_table:
            indent2 = ""

        if not isinstance(clause, list):
            clause = [clause]

        tables = defaultdict(lambda: ([], self.get_empty_parameters(style)))

        create_method = getattr(self, f"get_create_{type}")
        # group by "on"

        for item in clause:
            on = item["on"]
            items, params = tables[on]
            item = create_method(item, style, params)
            item = f"{indent2}ADD {prefix}{item}"
            items.append(item)

        if by_table:
            return tables

        results = []
        for table, items in tables.items():
            adds, params = items
            separator = "\n" if len(adds) > 1 else " "
            adds = self.combine(adds, separator=separator)
            on = self.format_identifier(table)
            results.append((f"{indent}ALTER TABLE {on}{separator}{adds}", params))
        return results

    def build_create_constraint(
        self,
        clause: Union[list, dict],
        style: ParameterStyle,
        depth: int = 0,
        params=None,
        by_table=False,
    ):
        """Builds $.create.constraint"""
        return self.build_create_table_item(
            "constraint", clause, style, depth, params, by_table
        )

    def build_create_index(
        self,
        clause: Union[list, dict],
        style: ParameterStyle,
        depth: int = 0,
        params=None,
    ):
        """Builds $.create.index"""
        indent = " " * self.INDENT * depth
        if isinstance(clause, list):
            # multiple indexes
            results = []
            for c in clause:
                results.extend(
                    self.build_create_index(c, style, depth=depth, params=params)
                )
                return results
        if isinstance(clause, dict):
            name = clause["name"]
            on = clause["on"]
            type = clause.get("type")
            if type:
                if type not in INDEX_TYPES:
                    raise ValueError(f'create index: invalid type "{type}"')
                type = INDEX_TYPES[type]
                type = f" USING {type}"
            else:
                # default (btree)
                type = ""
            on = self.format_identifier(on)
            name = self.format_identifier(name)
            concurrently = clause.get("concurrently", False)
            if concurrently:
                concurrently = " CONCURRENTLY "
            else:
                concurrently = " "
            columns = clause.get("columns")
            expression = clause.get("expression")
            if columns:
                expression = self.combine(
                    [self.format_identifier(c) for c in columns], separator=", "
                )
            elif expression:
                expression = self.get_expression(
                    expression, style, params, depth=depth, allow_subquery=False
                )
            return [
                (
                    f"{indent}CREATE INDEX{concurrently}{name} ON {on}{type} ({expression})",
                    [],
                )
            ]

        raise NotImplementedError()

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
            indexes = None
            temporary = False
            maybe = False
        elif isinstance(clause, list):
            # multiple tables
            results = []
            for c in clause:
                results.extend(
                    self.build_create_table(c, style, depth=depth, params=params)
                )
            return results
        else:
            # object
            name = clause["name"]
            columns = clause.get("columns", None)
            constraints = clause.get("constraints", None)
            indexes = clause.get("indexes", None)
            as_ = clause.get("as", None)
            temporary = clause.get("temporary", False)
            maybe = clause.get("maybe", False)

        if indexes:
            # only create non-unique and non-primary indexes directly
            # unique and primary key are created automatically as constraints
            # if they are specified here, ignore them
            indexes = [
                i
                for i in indexes
                if not i.get("primary", False) and not i.get("unique", False)
            ]
            new_indexes = []
            for i in indexes:
                if i.get("primary", False) or i.get("unique", False):
                    continue
                i["on"] = name
            new_indexex = indexes

        params = self.get_parameters(style, params)
        temporary = " TEMPORARY " if temporary else " "
        maybe = " IF NOT EXISTS " if maybe else " "
        name = self.format_identifier(name)
        index_queries = []
        if indexes:
            index_queries = self.build_create_index(
                indexes, style, depth=depth, params=params
            )
        if as_:
            # CREATE TABLE name AS (SELECT ...)
            subquery = self.get_subquery(as_, style, params, depth=depth)
            result = [
                (
                    f"{indent}CREATE{temporary}TABLE{maybe}{name} AS (\n{subquery}\n)",
                    params,
                )
            ]
            result.extend(index_queries)
            return result
        else:
            # CREATE TABLE name
            if not columns:
                return [(f"{indent}CREATE{temporary}TABLE{maybe}{name}", None)]
            # CREATE TABLE name (...columns, constraints...)
            items = self.get_create_table_items(
                columns, style, params, constraints=constraints, depth=depth + 1
            )
            result = [
                (f"{indent}CREATE{temporary}TABLE{maybe}{name} (\n{items}\n)", params)
            ]
            result.extend(index_queries)
            return result

    def get_subquery(self, data, style, params, depth=0) -> str:
        result = self.build(data, style, depth=depth + 1, params=params)
        if len(result) != 1:
            # expecting subquery to build to exactly one query for this to work
            raise ValueError(f"subquery: expecting one query but got:\n{result}\n")
        return result[0][0]

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
        indent=True,
        parens=False,
        depth: int = 0,
    ) -> str:
        if indent:
            indent = " " * self.INDENT * depth
        else:
            indent = ""

        if isinstance(expression, (int, float, bool)):
            # literal, cast to string
            result = str(expression)
            return f"{indent}{result}"

        if expression is None:
            return "{indent}NULL"

        if isinstance(expression, list):
            # assume identifier list
            result = self.format_identifier(expression)
            return f"{indent}{result}"

        if isinstance(expression, str):
            if expression == self.WILDCARD_CHARACTER:
                return f"{indent}{expression}"
            if (
                len(expression) > 1
                and expression[0] == expression[-1]
                and expression[0] in self.QUOTE_CHARACTERS
            ):
                # if quotes with ', ", or `, assume this is a literal
                char = expression[0]
                expression = expression[1:-1]
                result = self.add_parameter(expression, style, params)
                return f"{indent}{result}"
            else:
                # if unquoted, always assume an identifier
                result = self.format_identifier(expression)
                return f"{indent}{result}"

        lp = "(" if parens else ""
        rp = ")" if parens else ""
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
                return f"{indent}{key}"
            else:
                if self.is_command(key):
                    # subquery expression, e.g. {"select": "..."}
                    if not allow_subquery:
                        raise ValueError(
                            f'cannot build "{key}", subqueries not allowed in this expression'
                        )
                    subquery = self.get_subquery(value, style, params, depth=depth)
                    return f"{indent}{result}"

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
                                    indent=False,
                                    parens=True,
                                    depth=depth,
                                )
                                for arg in value
                            ],
                            separator=f" {key} ",
                        )
                        return f"{indent}{lp}{result}{rp}"
                    else:
                        # e.g. {"not": ...}
                        expression = self.get_expression(
                            value[0],
                            style,
                            params,
                            allow_subquery=allow_subquery,
                            indent=False,
                            parens=True,
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
                            f"{indent}{lp}{key} {expression}{rp}"
                            if left
                            else f"{indent}{lp}{expression} {key}{rp}"
                        )

                # special cases
                if key == "case":
                    raise NotImplementedError("case is not implemented yet")
                if key == "between":
                    val = self.get_expression(
                        value["value"],
                        style,
                        params,
                        allow_subquery,
                        depth,
                        indent=False,
                    )
                    min_value = self.get_expression(
                        value["min"], style, params, allow_subquery, depth, indent=False
                    )
                    max_value = self.get_expression(
                        value["max"], style, params, allow_subquery, depth, indent=False
                    )
                    symmetric = value.get("symmetric", False)
                    symmetric = " SYMMETRIC " if symmetric else " "
                    return (
                        f"{indent}{val} BETWEEN{symmetric}{min_value} AND {max_value}"
                    )
                if key == "identifier":
                    result = self.format_identifier(value)
                    return f"{indent}{result}"
                if key == "literal":
                    result = self.add_parameter(value, style, params)
                    return f"{indent}{result}"
                if key == "raw":
                    result = value
                    return f"{indent}{result}"

                # fallback assumption: a function expression, e.g. {"md5": "a"} -> md5("a")
                # user-defined functions can exist
                # functions can be qualified by a schema
                if not self.validate_function(key):
                    raise ValueError(f'"{key}" is not a valid function')
                if not value:
                    arguments = ""
                else:
                    if isinstance(value, list):
                        arguments = self.combine(
                            [
                                self.get_expression(
                                    arg,
                                    style,
                                    params,
                                    allow_subquery=allow_subquery,
                                    indent=False,
                                    depth=depth,
                                )
                                for arg in value
                            ],
                            separator=", ",
                        )
                    else:
                        arguments = self.get_expression(
                            value,
                            style,
                            params,
                            indent=False,
                            allow_subquery=allow_subquery,
                            depth=depth,
                        )
                return f"{indent}{key}({arguments})"

        raise ValueError(f"cannot format expression {expression}")

    def get_create_constraint(
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
            raise ValueError(f'constraint "{name}" must have: "type"')

        if type not in CONSTRAINT_TYPES:
            raise ValueError(f'constraint "{name}" type invalid: "{type}"')

        type = CONSTRAINT_TYPES[type]
        check = ""
        if constraint.get("check"):
            # check constraints use an expression
            check = constraint["check"]
            check = self.get_expression(
                check, style, params, depth=depth, indent=False, parens=True
            )
            check = f" {check}"
            columns = ""
        else:
            # non-check constraints must have columns
            if "columns" not in constraint:
                raise ValueError(f'{type} constraint: "{name}" must have: "columns"')
            columns = constraint["columns"]
            columns = self.combine(
                [self.format_identifier(c) for c in columns], separator=", "
            )
            columns = f" ({columns})"

        related = ""
        if type == "foreign key":
            related_name = constraint.get("related_name")
            related_columns = constraint.get("related_columns")
            if not related_name or not related_columns:
                raise ValueError(
                    f'"{type}" constraint must have "related_columns" and "related_name"'
                )

            related_name = self.format_identifier(related_name)
            related_columns = self.combine(
                [self.format_identifier(c) for c in related_columns], separator=", "
            )
            related = f" REFERENCES {related_name} ({related_columns})"

        deferrable = constraint.get("deferrable", False)
        deferred = constraint.get("deferred", False)
        deferrable = "DEFERRABLE" if deferrable else "NOT DEFERRABLE"
        deferred = "INITIALLY DEFERRED" if deferred else "INITIALLY IMMEDIATE"
        name = self.format_identifier(name)
        type = type.upper()  # costmetic
        return f"{indent}CONSTRAINT {name} {type}{check}{columns}{related} {deferrable} {deferred}"

    def get_create_column(
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
                default, style, params, indent=False, allow_subquery=False
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
            items.append(self.get_create_column(c, style, params, depth=depth))
        if constraints:
            for c in constraints:
                items.append(self.get_create_constraint(c, style, params, depth=depth))
        return self.combine(items, separator=",\n")
