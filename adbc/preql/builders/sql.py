from adbc.preql.dialect import ParameterStyle, get_default_style
import re
import copy
from collections import defaultdict
from typing import List, Union, Optional
from .core import Builder
from adbc.generators import G
from adbc.utils import flatten

# from .statements import Select, ...


def add_key(d, k, v):
    d = copy.copy(d)
    d[k] = v
    return d


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
CONSTRAINT_ABBREVIATIONS = {
    "primary": "pk",
    "foreign": "fk",
    "unique": "uk",
    "check": "ck",
    "exclude": "xk",
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

SCHEMA_ORDER = {
    "database": 1,
    "schema": 2,
    "table": 3,
    "sequence": 4,
    "index": 5,
    "column": 6,
    "constraint": 7,
}


class SQLBuilder(Builder):
    def get_default_style(self):
        return ParameterStyle.FORMAT

    def add_parameter(
        self, value, style: ParameterStyle, params: Union[list, dict], name=None
    ):
        num = len(params) + 1
        if name and not style == ParameterStyle.NAMED:
            raise ValueError("can only add named parameters with style NAMED")

        if style == ParameterStyle.NAMED:
            label = name if name else f"p{num}"
            params[param] = value
            return f":{label}"
        elif style == ParameterStyle.DOLLAR_NAMED:
            label = name if name else f"p{num}"
            params[param] = value
            return f"${label}"
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

    def normalize(self, query: Union[list, dict]):
        if isinstance(query, list):
            return [self.normalize(q) for q in query]

        if "alter" in query:
            query = self.normalize_alter(query)

        return query

    def build(
        self,
        query: Union[list, dict],
        style: ParameterStyle = None,
        depth: int = 0,
        params=None,
        normalize=True,
    ) -> List[tuple]:
        if normalize:
            query = self.normalize(query)

        if isinstance(query, list):
            results = []
            for q in query:
                for result in self.build(q, style, depth, params, normalize=False):
                    results.extend(result)
            return results

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

    def build_drop(
        self,
        clause: Union[List, dict, str],
        style: ParameterStyle,
        depth: int = 0,
        params=None,
    ) -> List[tuple]:
        """Builds $.drop

        Can drop the following:
            database: (DROP DATABASE)
            schema: (DROP SCHEMA)
            table: (DROP TABLE)
            column (ALTER TABLE DROP COLUMN)
            constraint (ALTER TABLE DROP CONSTRAINT)
            sequence (DROP SEQUENCE)
        """
        indent = self.get_indent(depth)
        if isinstance(clause, list):
            results = []
            for c in clause:
                results.extend(self.build_drop(c, style, depth=depth, params=params))
            return results

        children = (
            "database",
            "schema",
            "table",
            "sequence",
            "column",
            "constraint",
            "index",
        )
        method = None
        for child in children:
            if child in clause:
                method = f"build_drop_{child}"
                break

        if method:
            method = getattr(self, method)
            return method(clause[child], style, depth=depth, params=params)
        else:
            raise NotImplementedError(f"drop expecting to contain one of: {children}")
        raise NotImplementedError()

    def build_drop_database(
        self, clause: Union[dict, list, str], style, params=None, depth=0
    ):
        return self.build_drop_schema_item(
            "database", clause, style, params, depth=depth
        )

    def build_drop_schema_item(
        self, type, clause: Union[dict, list, str], style, params=None, depth=0
    ):
        if isinstance(clause, list):
            return flatten(
                [self.build_drop_schema_item(type, c, style, params) for c in clause]
            )
        if isinstance(clause, str):
            name = clause
            maybe = False
            cascade = False
        if isinstance(clause, dict):
            name = clause.get("name")
            maybe = clause.get("maybe", False)
            cascade = clause.get("cascade", False)

        if not name:
            raise ValueError(f"drop.{type}: name is required")

        name = self.format_identifier(name)
        maybe = " IF EXISTS " if maybe else " "
        cascade = " CASCADE" if cascade else ""
        if type == "database":
            # database does not support cascade
            # silently ignore it in case it is passed
            cascade = ""
        type = type.upper()  # database -> DATABASE
        query = f"DROP {type}{maybe}{name}{cascade}"
        return [(query, params)]

    def build_drop_schema(self, clause, style, params=None, depth=0):
        return self.build_drop_schema_item("schema", clause, style, params, depth=depth)

    def build_drop_table(self, clause, style, params, depth=0):
        return self.build_drop_schema_item("table", clause, style, params, depth=depth)

    def build_drop_sequence(self, clause, style, params, depth=0):
        return self.build_drop_schema_item(
            "sequence", clause, style, params, depth=depth
        )

    def build_drop_column(self, clause, style, params, depth=0):
        return self.build_drop_table_item("column", clause, style, params, depth=depth)

    def build_drop_table_item(self, type, clause, style, params, depth=0):
        if isinstance(clause, list):
            return flatten(
                [self.build_drop_table_item(type, c, style, params) for c in clause]
            )
        else:
            item = clause.get("name") if isinstance(clause, dict) else clause
            parts = self.unpack_identifier(item)
            name = parts[-1]

            if isinstance(clause, dict):
                clause["name"] = name
            else:
                clause = name
            num_parts = len(parts)
            if num_parts >= 2:
                table = self.combine(parts[0 : num_parts - 1], separator=".")
            else:
                # 1 part only
                raise ValueError(
                    f'drop.{type}: cannot identify "{item}", needs table part'
                )
            return self.build(
                {"alter": {"table": {"name": table, "drop": {type: clause}}}},
                style,
                depth,
                params,
            )

    def build_drop_constraint(self, clause, style, params, depth=0):
        # rewrite as {"alter": {"table": {"drop": ...}}}
        return self.build_drop_table_item(
            "constraint", clause, style, params, depth=depth
        )

    def build_drop_index(self, clause, style, params, depth=0):
        return self.build_drop_schema_item("index", clause, style, params, depth=depth)

    def build_truncate(
        self,
        clause: Union[list, dict, str],
        style: ParameterStyle,
        depth: int = 0,
        params=None,
    ) -> List[tuple]:
        indent = self.get_indent(depth)
        if isinstance(clause, list):
            return flatten(
                [self.build_truncate(c, style, depth, params) for c in clause]
            )
        if isinstance(clause, str):
            name = clause
            cascade = False
        else:
            name = clause.get("name")
            cascade = clause.get("cascade", False)

        cascade = " CASCADE" if cascade else ""
        name = self.format_identifier(name)
        query = f"{indent}TRUNCATE {name}{cascade}"
        return [(query, params)]

    def build_insert(
        self,
        clause: dict,
        style: ParameterStyle,
        depth: int = 0,
        params=None,
    ) -> List[tuple]:
        """Builds $.insert

        Arguments:
            clause: dict
                with: dict
                table: identifier
                values: ?list
                columns: ?list
                return: list[identifier]
        """
        indent = self.get_indent(depth)
        if isinstance(clause, str):
            values = None
            columns = None
            table = clause
            with_ = None
            returning = None
        else:
            values = clause.get("values")
            columns = clause.get('columns')
            table = clause.get("table")
            with_ = clause.get("with")
            returning = clause.get("return")

        if not table:
            raise ValueError("insert: table is required")

        # sub-clausal order: with, into, columns, values, returning
        # TODO: process values

        # with: common table expressions
        with_ = (
            (self.get_with(with_, style, params=params, depth=depth) + f"\n{indent}")
            if with_
            else ""
        )

        if columns:
            columns = self.parens(
                self.combine(
                    [self.format_identifier(column) for column in columns],
                    separator=', '
                )
            )

        values = self.get_values(values, style, params=params, depth=depth)
        # Returning: Postgres-only
        # returns output rows based on updated rows
        # support same syntax as select data
        returning = self.get_returning(returning, style, params)

        table = self.format_identifier(table)
        rest = self.combine([values, returning], separator="\n", check=True)
        columns = f' {columns}' if columns else ''
        rest = f"\n{rest}" if rest else ""
        return [(f"{indent}{with_}INSERT INTO {table}{columns}{rest}", params)]

    def get_values(self, values, style, params, depth=0):
        indent2 = self.get_indent(depth+1)
        if not values:
            return 'DEFAULT VALUES'

        elif isinstance(values, list):
            if isinstance(values[0], list):
                # many values lists
                values = self.combine([
                        self.parens(self.combine([
                            self.get_expression(v, style, params, allow_subquery=False)
                            for v in value
                        ], separator=', '))
                        for value in values
                    ], separator=f',\n{indent2}'
                )
                return f'VALUES\n{indent2}{values}'
            else:
                # one values list
                values = self.parens(self.combine(
                    [self.get_expression(value, style, params, allow_subquery=False) for value in values],
                    separator=', '
                ))
                return f'VALUES {values}'
        elif isinstance(values, dict):
            # values subquery
            # Postgres only
            return self.get_subquery(values, style, params, depth=depth)
        else:
            raise ValueError(f'insert: invalid values: {values}')

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
        indent = self.get_indent(depth)
        indent2 = self.get_indent(depth + 1)

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
            for name, results in table_results.items():
                name = self.format_identifier(name)
                results, params = results
                separator = f",\n{indent2}"
                sep0 = f"\n{indent2}" if len(results) > 1 else " "
                results = self.combine(results, separator=separator)
                results = f"{indent}ALTER TABLE {name}{sep0}{results}"
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
            by_table = by_table and (child == "column" or child == "constraint")
            if by_table:
                kwargs["by_table"] = True
            return method(clause[child], style, **kwargs)
        else:
            raise NotImplementedError(f"create expecting to contain one of: {children}")

    def normalize_alter(self, clause: dict):
        """Normalizes an alter clause:

        - table alters on the same table are merged
        - column/constraints alters in the same table are grouped with table alters
        - any renames are separated and performed last in reverse schema order

        Example:

        Before normalization:

            {
                "alter": [{
                    "table": {
                        "name": "A",
                        "add": {
                            "column": {
                                "name": "id",
                                "type": "integer"
                            }
                        },
                        "alter": {
                            "column": {
                                "name": "name",
                                "null": True
                            }
                        }
                    }
                },{
                    "table": [{
                        "name": "A",
                        "rename": `"B"
                    }, {
                        "name": "A",
                        "drop": {
                            "constraint": "foo_uk",
                        }
                    }]
                }, {
                    "column": {
                        "name": "name",
                        "on": "A",
                        "default": "'foo'",
                        "rename": "new_name"
                    }
                }]
            }

        After normalization:

            {
                "alter": [{
                    "table": {
                        "name": "A",
                        "add": {
                            "column": {
                                "name": "id",
                                "type": "integer"
                            }
                        },
                        "alter": {
                            "column": {
                                "name": "name",
                                "null": True,
                                "default": "'foo'"
                            }
                        },
                        "drop": {
                            "constraint": "foo_uk"
                        }
                    }
                }, {
                    "column": {
                        "name": "name",
                        "on": "A",
                        "rename": "name2"
                    }
                }, {
                    "table": {
                        "name": "A",
                        "rename": "B"
                    }
                }]
            }
        """

        # - table alters on the same table are merged
        # - column/constraints alters in the same table are grouped with table alters
        # - any renames are separated and performed last in the order of:
        #    - constraint
        #    - column
        #    - table
        #    - schema
        alter = next(iter(clause.values()))
        if isinstance(alter, dict):
            alters = [alter]
        else:
            alters = alter

        by_table = {}  # keyed by table name
        renames = defaultdict(list)  # keyed by schema type (e.g. database, table)
        by_other = defaultdict(list)  # keyed by schema type
        for alter in alters:
            type, clause = next(iter(alter.items()))
            if isinstance(clause, list):
                clauses = clause
            else:
                clauses = [clause]
            for clause in clauses:
                name = clause.get("name")
                if "rename" in clause:
                    clause.pop("name")  # to check for empty
                    on = clause.pop("on", None)
                    rename = clause.pop("rename")
                    if clause:
                        # a rename bundled with another changes
                        # these have to be separated in postgres
                        rename = {"name": name, "rename": rename}
                        if on:
                            rename["on"] = on
                            clause["on"] = on
                        clause["name"] = name
                        # continue processing "clause" without rename
                        renames[type].append(rename)
                    else:
                        # strictly a rename clause
                        clause["name"] = name
                        clause["rename"] = rename
                        if on:
                            clause["on"] = on
                        renames[type].append(clause)
                        continue

                if type == "column" or type == "constraint":
                    # re-express {"alter": {"column": {"on": "A", ...}}
                    # as {"alter": {"table": {"name": "A", "alter": {"column": {...}}}
                    name = clause.pop("on")
                    clause = {"name": name, "alter": {type: clause}}
                    type = "table"

                if type == "table":
                    if "alter" in clause:
                        # separate renames
                        for type2, clause2 in clause["alter"].items():
                            name2 = clause2.get("name")
                            if isinstance(clause2, list):
                                clauses2 = clause2
                            else:
                                clauses2 = [clause2]
                            for clause2 in clauses2:
                                if "rename" in clause2:
                                    clause2.pop("name")
                                    rename = clause2.pop("rename")
                                    if clause2:
                                        rename = {
                                            "name": name2,
                                            "rename": rename,
                                            "on": name,
                                        }
                                        clause2["name"] = name2
                                        renames[type2].append(rename)
                                    else:
                                        clause2["name"] = name2
                                        clause2["rename"] = rename
                                        clause2["on"] = name

                                        renames[type2].append(clause2)
                                        continue

                    if name in by_table:
                        # merge alter tables
                        old = by_table[name]
                        for key in ("add", "drop", "alter"):
                            if key in clause:
                                new_value = clause[key]
                                if key in old:
                                    old_value = old[key]
                                    # merge
                                    if key == "alter":
                                        # alter: merge by type and name
                                        # e.g:
                                        # {"alter": {"column": {"name": "A", "null": True}} and
                                        # {"alter": {"column": {"name": "A", "default": 1}} ->
                                        # {"alter": {"column": {"name": "A", "default": 1, "null": True}}
                                        for type2, action in new_value.items():
                                            merged = {}
                                            if not isinstance(action, list):
                                                action = [action]

                                            old_names = {}
                                            new_names = {}

                                            for o in old_value.get(type2, []):
                                                name2 = o["name"]
                                                if name2 in old_names:
                                                    old_names[name2].update(o)
                                                else:
                                                    old_names[name2] = o

                                            for o in action:
                                                name2 = o["name"]
                                                if name2 in new_names:
                                                    new_names[name2].update(o)
                                                else:
                                                    new_names[name2] = o

                                            for (
                                                new_name,
                                                new_value,
                                            ) in new_names.items():
                                                if new_name in old_names:
                                                    # merge values with dict update
                                                    merged_value = old_names.pop(
                                                        new_name
                                                    )
                                                    merged_value.update(new_value)
                                                    merged[new_name] = merged_value
                                                else:
                                                    # use new value
                                                    merged[new_name] = new_value
                                            merged.update(old_names)

                                            if len(merged) == 1:
                                                merged = next(iter(merged.values()))
                                            else:
                                                merged = list(merged.values())
                                            old_value[type2] = merged
                                    else:
                                        # add, drop: merge by type
                                        # e.g:
                                        # {"add": {"column": {...A...}} +
                                        # {"add": {"column": {...B...}} ->
                                        # {"add": {"column": [{...A...}, {...B...}]}
                                        for type2, action in new_value.items():
                                            if not isinstance(old_value[type2], list):
                                                old_value[type2] = [old_value[type2]]

                                            if isinstance(action, list):
                                                old_value[type2].extend(action)
                                            else:
                                                old_value[type2].append(action)
                                else:
                                    old[key] = new_value

                    else:
                        by_table[name] = clause

                elif type in {"index", "sequence", "schema", "database"}:
                    by_other[type].append(clause)

        normalized = []
        by_other["table"] = None
        for type, clauses in sorted(by_other.items(), key=lambda x: SCHEMA_ORDER[x[0]]):
            # in order: database, schema, table, ..., column, constraint
            if type == "table":
                clauses = list(by_table.values())
                if len(clauses) == 1:
                    clauses = clauses[0]
            normalized.append({type: clauses})

        for type, clauses in sorted(
            renames.items(), key=lambda x: -1 * SCHEMA_ORDER[x[0]]
        ):
            # in order: constraint, column,..., table, schema, database
            normalized.append({type: clauses})
        if len(normalized) == 1:
            normalized = normalized[0]
        return {"alter": normalized}

    def build_alter(
        self, clause: dict, style: ParameterStyle, params=None, depth: int = 0,
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
        indent = self.get_indent(depth)
        if isinstance(clause, list):
            results = []
            for c in clause:
                if params is not None:
                    p = copy.copy(params)
                else:
                    p = params
                results.extend(self.build_alter(c, style, depth=depth, params=p,))
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
                method = f"build_alter_{child}"
                break

        if method:
            method = getattr(self, method)
            return method(clause[child], style, depth=depth, params=params)
        else:
            raise NotImplementedError(f"alter expecting to contain one of: {children}")

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

    def parens(self, value):
        return f'({value})'

    def combine(self, segments, separator="\n", check=False):
        if check:
            return separator.join([s for s in segments if s])
        return separator.join(segments)

    def get_select_with(self, with_, style, params, prefix=True, depth=0) -> str:
        return self.get_with(with_, style, params, prefix=prefix, depth=depth)

    def get_with(self, with_, style, params, prefix=True, depth=0) -> str:
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
                self.get_with(w, style, params, prefix=False, depth=depth + 1)
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
            return f"{prefix}{as_} AS (\n{subquery}\n)"

        raise NotImplementedError()

    def get_select_data(self, data, style, params, prefix=True, depth=0) -> str:
        # string       ("*")                                 # single literal or identifier value
        # dict         ({"name": "first_name"})              # fully aliased expression list
        # list[]       ([...])                               # combination of aliased and unaliased values
        if not data:
            raise ValueError('select: must have "data"')

        indent = self.get_indent(depth)
        indent2 = self.get_indent(depth + 1)

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
                separator=f",\n{indent2}",
            )
            if prefix:
                return f"{indent}SELECT\n{indent2}{result}"
            else:
                return result

        if isinstance(data, str):
            result = self.get_expression(
                data, style, params, indent=False, allow_subquery=False
            )
            if prefix:
                return f"{indent}SELECT {result}"
            else:
                return result

        if isinstance(data, dict):
            results = []
            for name, value in data.items():
                name = self.format_identifier(name)
                expression = self.get_expression(
                    value, style, params, indent=False, allow_subquery=True
                )
                results.append(f"{expression} AS {name}")
            result = self.combine(results, separator=f",\n{indent2}")
            if prefix:
                return f"{indent}SELECT\n{result}"
            else:
                return result

        raise NotImplementedError()

    def get_select_from(self, from_, style, params, prefix=True, depth=0):
        # string       ("users")                  # table by name
        # dict[string] ({"u": "users"})           # aliased name
        # dict[dict]   ({"u": {"select": ...}})   # aliased subquery
        #              ({"u": {"lateral": {...}}  # modifier e.g. LATERAL
        # list         ([...])                    # list of the above
        indent = self.get_indent(depth)
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
            results = []
            for name, target in from_.items():
                name = self.format_identifier(name)
                if isinstance(target, str):
                    target = self.format_identifier(target)

                elif isinstance(target, dict):
                    target_key = next(iter(target.keys()))
                    if self.is_command(target_key):
                        # subquery in FROM
                        # TODO: support for LATERAL
                        target = self.get_subquery(target, style, params, depth=depth)
                        target = f"(\n{target}\n{indent})"
                    else:
                        # function in FROM, e.g. FROM generate_series(...) AS S
                        target = self.get_expression(target, style, params, depth=depth)

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
        indent = self.get_indent(depth)

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
                to = f"(\n{to}\n{indent})"
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
        indent = self.get_indent(depth)
        where = self.get_expression(where, style, params, indent=False, depth=depth)
        return f"{indent}WHERE {where}"

    def get_select_group(self, group, style, params, prefix=True, depth=0) -> str:
        # string       ("name")                         # simple group by (no rollup)
        # dict         ({"by": "name", "rollup": True}) # group by condition
        # list[dict]   ([...])                          # list of conditions
        indent = self.get_indent(depth)
        if prefix:
            prefix = f"{indent}GROUP BY "
        else:
            prefix = ""

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
            group = self.format_identifier(group)
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
        indent = self.get_indent(depth)
        having = self.get_expression(having, style, params, indent=False, depth=depth)
        return f"{indent}HAVING {having}"

    def get_select_union(self, union, style, params, depth=0) -> str:
        # - union:   dict         ({"select": ...})                     # union query
        #            list[dict]   ([...])
        if not union:
            return None

        indent = self.get_indent(depth)
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
        indent = self.get_indent(depth)
        if prefix:
            prefix = f"{indent}ORDER BY "
        else:
            prefix = ''

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
            return f"{prefix}{order}"
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
        indent = self.get_indent(depth)
        limit = int(limit)
        return f"{indent}LIMIT {limit}"

    def get_select_offset(self, offset, style, params, depth=0) -> str:
        # - offset:  integer      (1)                                   # an integer
        if offset is None:
            return None
        indent = self.get_indent(depth)
        offset = int(offset)
        return f"{indent}OFFSET {offset}"

    def get_indent(self, depth=0):
        return self.WHITESPACE_CHARACTER * self.INDENT * depth

    def get_returning(
        self, returning: Union[list, str, dict], style, params, prefix=True
    ):
        # TODO: merge with get_select_data? Postgres says they are identical
        if returning is None:
            return returning

        wild = self.WILDCARD_CHARACTER
        result = []
        if isinstance(returning, str):
            # e.g: "*" or "name"
            if returning != wild:
                returning = self.format_identifier(returning)
            result = [returning]
        elif isinstance(returning, list):
            # e.g.: ["*", {"a": {...}}]
            result = [
                self.get_returning(r, style, params, prefix=False) for r in returning
            ]
        elif isinstance(returning, dict):
            # e.g. {"a": {...}}
            for name, expr in returning.items():
                name = self.format_identifier(name)
                expr = self.get_expression(expr, style, params,)
                result.append(f"{expr} AS {name}")

        result = self.combine(result, separator=", ")
        return f"RETURNING {result}" if prefix else result

    def build_delete(
        self, clause: Union[dict, str], style: ParameterStyle, params, depth=0
    ) -> List[tuple]:
        """Builds $.delete

        Arguments:
            clause: dict
                table: identifier
                where: expression
                returning: list[identifier]
        """
        indent = self.get_indent(depth)
        if isinstance(clause, str):
            where = None
            returning = None
            table = clause
        else:
            where = clause.get("where")
            returning = clause.get("return")
            table = clause.get("table")

        if not table:
            raise ValueError("delete: table is required")

        table = self.format_identifier(table)
        where = (
            self.get_expression(where, style, params, depth=depth) if where else None
        )
        where = f"WHERE {where}" if where else None
        returning = self.get_returning(returning, style, params)
        rest = self.combine([where, returning], separator="\n", check=True)
        rest = f"\n{rest}" if rest else ""
        return [(f"{indent}DELETE FROM {table}{rest}", params)]

    def get_update_set(self, clause: dict, style: ParameterStyle, params, depth=0):
        indent = self.get_indent(depth)
        result = []
        if isinstance(clause, dict):
            # General SQL syntax SET column = expr
            # in PreQL: {"a": expr, "b": expr}
            for name, value in clause.items():
                name = self.format_identifier(name)
                value = self.get_expression(value, style, params, indent=False)
                result.append(f"{name} = {value}")
            result = self.combine(result, separator=f",\n{indent}")
        elif isinstance(clause, list):
            # Postgres only syntax: SET (columnA, columnB) = (subquery)
            # in PreQL: ["a", "b", "c", subquery]
            subquery = clause[-1]
            columns = clause[0:-1]
            columns = self.combine(
                [self.format_identifier(c) for c in columns], separator=", "
            )
            subquery = self.get_subquery(subquery, style, params, depth=depth)
            result = f"({columns}) = (\n{subquery}\n{indent})"
        return f"{indent}{result}"

    def build_update(
        self, clause: dict, style: ParameterStyle, params, depth=0
    ) -> List[tuple]:
        """Builds $.update

        Arguments:
            clause: dict
                table: identifier
                set: dict[str, expression]
                from: union[list, str]
                where: expression
                return: list[identifier]
        """
        indent = self.get_indent(depth)
        from_ = clause.get("from")
        where = clause.get("where")
        set_ = clause.get("set")
        table = clause.get("table")
        with_ = clause.get("with")
        returning = clause.get("return")
        if not set_:
            raise ValueError("update: set is required")

        # sub-clausal order: with, set, from, where, returning

        # with: common table expressions
        with_ = (
            (self.get_with(with_, style, params=params, depth=depth) + f"\n{indent}")
            if with_
            else ""
        )

        set_ = self.get_update_set(set_, style, params, depth=depth + 1)
        if from_:
            if not isinstance(from_, list):
                from_ = [from_]
            # TODO: support update with subquery in FROM
            from_ = self.combine(
                [self.format_identifier(f) for f in from_], separator=", "
            )
            from_ = f"FROM {from_}"

        if where:
            where = self.get_expression(where, style, params, depth=depth)
            where = f"WHERE {where}" if where else None

        # Returning: Postgres-only
        # returns output rows based on updated rows
        # support same syntax as select data
        returning = self.get_returning(returning, style, params)

        table = self.format_identifier(table)
        rest = self.combine([from_, where, returning], separator="\n", check=True)
        rest = f"\n{rest}" if rest else ""
        return [(f"{indent}{with_}UPDATE {table}\nSET\n{set_}{rest}", params)]

    def escape_literal(self, literal):
        quote = self.LITERAL_QUOTE_CHARACTER
        if quote in literal:
            literal = re.sub(quote, f"{quote}{quote}", literal)
        return f"{quote}{literal}{quote}"

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
        indent = self.get_indent(depth)
        database = self.format_identifier(clause)
        query = f"{indent}CREATE DATABASE {database}"
        return [(query, params)]

    def build_create_schema(
        self, clause: str, style: ParameterStyle, depth: int = 0, params=None
    ) -> List[tuple]:
        """Builds $.create.schema"""
        indent = self.get_indent(depth)
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
        indent = self.get_indent(depth)
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
        temporary = "TEMPORARY" if temporary else None
        maybe = "IF NOT EXISTS" if maybe else None
        owned_by = f"OWNED BY {owned_by}" if owned_by else None
        min_value = f"MINVALUE {min_value}" if min_value else None
        max_value = f"MAXVALUE {max_value}" if max_value else None
        start = f"START WITH {start}" if start else None
        increment = f"INCREMENT BY {increment}" if increment else None
        statement = self.combine(
            [
                "CREATE",
                temporary,
                "SEQUENCE",
                maybe,
                name,
                increment,
                min_value,
                max_value,
                start,
                owned_by,
            ],
            separator=" ",
            check=True,
        )
        query = f"{indent}{statement}"
        return [(query, params)]

    def build_alter_column(
        self,
        clause: Union[list, dict],
        style: ParameterStyle,
        depth: int = 0,
        params=None,
    ):
        return self.build_alter_table_item(
            "column", clause, style, depth=depth, params=params
        )

    def build_alter_constraint(
        self,
        clause: Union[list, dict],
        style: ParameterStyle,
        depth: int = 0,
        params=None,
    ):
        return self.build_alter_table_item(
            "constraint", clause, style, depth=depth, params=params
        )

    def build_alter_table_item(
        self,
        type,
        clause: Union[list, dict],
        style: ParameterStyle,
        depth: int = 0,
        params=None,
    ):
        if isinstance(clause, list):
            clauses = clause
            results = []
            for clause in clauses:
                results.extend(
                    self.build_alter_table_item(type, clause, style, depth, params)
                )
            return results

        if type == "column":
            prefix = "COLUMN "
        else:
            prefix = "CONSTRAINT "

        indent = self.get_indent(depth)
        alter_getter = getattr(self, f"get_alter_{type}")
        if "on" not in clause:
            raise ValueError(f'alter {type}: must identify table with "on" {clause}')

        table = self.format_identifier(clause["on"])
        alter = alter_getter(clause, style, params)
        return [(f"{indent}ALTER TABLE {table} {alter}", params)]

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
            prefix = f"COLUMN "
        else:
            prefix = f"CONSTRAINT "

        indent = self.get_indent(depth)
        indent2 = self.get_indent(depth + 1)
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
            item = f"ADD {prefix}{item}"
            items.append(item)

        if by_table:
            return tables

        results = []
        for table, items in tables.items():
            adds, params = items
            separator = ",\n{indent2}"
            sep0 = "\n{indent2}" if len(adds) > 1 else " "
            adds = self.combine(adds, separator=separator)
            on = self.format_identifier(table)
            results.append((f"{indent}ALTER TABLE {on}{sep0}{adds}", params))

        return results

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
            "constraint", clause, style, depth=depth, params=params, by_table=by_table
        )

    def build_create_index(
        self,
        clause: Union[list, dict],
        style: ParameterStyle,
        depth: int = 0,
        params=None,
    ):
        """Builds $.create.index"""
        indent = self.get_indent(depth)
        if isinstance(clause, list):
            # multiple indexes
            results = []
            for c in clause:
                results.extend(
                    self.build_create_index(c, style, depth=depth, params=params)
                )
                return results

        if not isinstance(clause, dict):
            raise NotImplementedError()

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
        concurrently = " CONCURRENTLY " if clause.get("concurrently") else ""
        columns = clause.get("columns")
        expression = clause.get("expression")
        maybe = " IF NOT EXISTS " if clause.get("maybe") else ""
        if not concurrently and not maybe:
            # add space before name
            name = f" {name}"
        unique = " UNIQUE " if clause.get("unique") else " "
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
                f"{indent}CREATE{unique}INDEX{concurrently}{maybe}{name} ON {on}{type} ({expression})",
                params,
            )
        ]

    def add_auto_constraints(
        self,
        table: str,
        columns: Optional[List[dict]],
        constraints: Optional[List[dict]],
    ):
        if columns is None:
            return constraints

        constraints = constraints or []
        constraint_names = {constraint["name"] for constraint in constraints}
        table = table.split(".")[-1]  # strip schema
        # return original input if no changes were made
        for column in columns:
            column_name = column["name"]
            primary = column.get("primary", False)
            unique = column.get("unique", False)
            related = column.get("related", None)
            if primary:
                changes = True
                if not isinstance(primary, str):
                    primary = self.get_auto_constraint_name(
                        table, column_name, "primary"
                    )
                if primary not in constraint_names:
                    constraint = G("constraint", type="primary", columns=[column_name])
                    constraint["name"] = primary
                    constraints.append(constraint)
                    constraint_names.add(primary)
            if unique:
                changes = True
                if not isinstance(unique, str):
                    unique = self.get_auto_constraint_name(table, column_name, "unique")
                if unique not in constraint_names:
                    constraint = G("constraint", type="unique", columns=[column_name])
                    constraint["name"] = unique
                    constraints.append(constraint)
                    constraint_names.add(unique)
            if related:
                if "to" not in related or "by" not in related:
                    raise ValueError('column.related: must have "to" and "by"')

                changes = True
                to = related["to"]
                by = related["by"]
                if "name" in related:
                    name = related["name"]
                else:
                    name = self.get_auto_constraint_name(table, column_name, "foreign")

                constraint = G(
                    "constraint",
                    type="foreign",
                    columns=[column_name],
                    related_name=[to],
                    related_columns=[by],
                )
                constraint["name"] = name
                constraints.append(constraint)
                constraint_names.add(name)

        return constraints

    def get_auto_constraint_name(self, table, name, type):
        suffix = CONSTRAINT_ABBREVIATIONS[type]
        return f"{table}__{name}__{suffix}"

    def build_alter_table(
        self,
        clause: Union[list, dict],
        style: ParameterStyle,
        depth: int = 0,
        params=None,
    ):
        """Builds $.alter.table"""
        if isinstance(clause, list):
            clauses = clause
            results = []
            for clause in clauses:
                results.extend(
                    self.build_alter_table(clause, style, depth=depth, params=params)
                )
            return results

        indent = self.get_indent(depth)
        indent2 = self.get_indent(depth + 1)
        rename = clause.get("rename", None)
        add = clause.get("add", None)
        alter = clause.get("alter", None)
        drop = clause.get("drop", None)
        name = clause.get("name", None)
        if not name:
            raise ValueError("alter.table: name is required")

        table = self.format_identifier(name)
        actions = []

        for action, value in (
            ("add", add),
            ("drop", drop),
            ("alter", alter),
            ("rename", rename),
        ):
            result = getattr(self, f"get_alter_table_{action}")(value, style, params)
            actions.extend(result)

        if not actions:
            raise ValueError("alter.table: must have rename/add/alter/drop actions")

        separator = f",\n{indent2}"
        sep0 = " " if len(actions) == 1 else f"\n{indent2}"
        actions = self.combine(actions, separator=separator)
        return [(f"{indent}ALTER TABLE {table}{sep0}{actions}", params)]

    def get_alter_table_add(self, clause: dict, style, params):
        results = []
        if not clause:
            return results
        for type, action in clause.items():
            # e.g. type, action = "column", {"name": "foo", ...}
            if not isinstance(action, list):
                actions = [action]
            else:
                actions = action

            upper_type = type.upper()
            for action in actions:
                method = getattr(self, f"get_create_{type}")
                result = method(action, style, params)
                results.append(f"ADD {upper_type} {result}")
        return results

    def get_alter_table_drop(self, clause: dict, style, params):
        results = []
        if not clause:
            return results
        for type, action in clause.items():
            # e.g. type, action = "column", ["name", "first"]
            if not isinstance(action, list):
                actions = [action]
            else:
                actions = action

            upper_type = type.upper()
            for action in actions:
                if isinstance(action, str):
                    name = action
                else:
                    name = action["name"]
                name = self.format_identifier(name)
                results.append(f"DROP {upper_type} {name}")
        return results

    def get_alter_table_alter(self, clause: dict, style, params):
        results = []
        if not clause:
            return results
        for type, action in clause.items():
            # e.g. type, action = "column", {"name": "foo", "null": True}
            if not isinstance(action, list):
                actions = [action]
            else:
                actions = action
            for action in actions:
                method = getattr(self, f"get_alter_{type}")
                result = method(action, style, params)
                results.append(result)
        return results

    def get_alter_table_rename(self, rename: str, style, params):
        if rename is None:
            return []
        rename = self.format_identifier(rename)
        return [f"RENAME TO {rename}"]

    def build_create_table(
        self,
        clause: Union[list, str, dict],
        style: ParameterStyle,
        depth: int = 0,
        params=None,
    ) -> List[tuple]:
        """Builds $.create.table"""

        indent = self.get_indent(depth)
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

        if isinstance(columns, dict):
            columns = [add_key(c, "name", name) for name, c in columns.items()]
        if isinstance(indexes, dict):
            indexes = [add_key(c, "name", name) for name, c in indexes.items()]
        if isinstance(constraints, dict):
            constraints = [add_key(c, "name", name) for name, c in constraints.items()]

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
                new_indexes.append(i)
            indexes = new_indexes

        # add automatic constraints
        constraints = self.add_auto_constraints(name, columns, constraints)

        params = self.get_parameters(style, params)
        temporary = " TEMPORARY " if temporary else " "
        maybe = " IF NOT EXISTS " if maybe else " "
        raw_name = name
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
            # add sequence queries for column "sequence" values
            sequence_queries = self.get_auto_sequence_queries(raw_name, columns, style)
            result.extend(sequence_queries)
            result.extend(index_queries)
            return result

    def get_auto_sequence_name(self, table_name, column_name):
        suffix = "seq"
        return f"{table_name}__{column_name}__{suffix}"

    def get_auto_sequence_queries(self, table_name: str, columns: List[dict], style):
        queries = []
        for column in columns:
            name = column.get("name")
            sequence = column.get("sequence")
            if sequence:
                if not isinstance(sequence, str):
                    sequence = self.get_auto_sequence_name(table_name, name)

                sequence_name = sequence
                sequence = {
                    "name": sequence_name,
                    "maybe": True,
                    "owned_by": f"{table_name}.{name}",
                }
                # CREATE SEQUENCE IF NOT EXISTS ...
                queries.extend(
                    self.build(
                        {
                            "create": {
                                "sequence": {
                                    "name": sequence_name,
                                    "maybe": True,
                                    "owned_by": f"{table_name}.{name}",
                                }
                            },
                        },
                        style,
                    )
                )
                # ALTER TABLE ... ALTER COLUMN ... DEFAULT nextval(...)
                queries.extend(
                    self.build(
                        {
                            "alter": {
                                "column": {
                                    "name": name,
                                    "on": table_name,
                                    "default": {"nextval": f"`{sequence_name}`"},
                                }
                            }
                        },
                        style,
                    )
                )
        return queries

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

    def validate_type(self, name):
        if re.match(r"^[a-zA-Z][A-Za-z0-9\[\] ]*", name):
            return True
        return False

    def validate_keyword(self, name):
        if re.match(r"^[A-Za-z][A-Za-z_]*$", name):
            return True
        return False

    def get_between_expression(
        self, value, style, params, allow_subquery=False, depth=0,
    ):
        val = self.get_expression(
            value["value"], style, params, allow_subquery, depth=depth, indent=False,
        )
        indent = self.get_indent(depth)
        min_value = self.get_expression(
            value["min"], style, params, allow_subquery, depth, indent=False
        )
        max_value = self.get_expression(
            value["max"], style, params, allow_subquery, depth, indent=False
        )
        symmetric = value.get("symmetric", False)
        symmetric = " SYMMETRIC " if symmetric else " "
        return f"{indent}{val} BETWEEN{symmetric}{min_value} AND {max_value}"

    def get_in_expression(
        self, value, style, params, allow_subquery=False, depth=0,
    ):
        indent = self.get_indent(depth)
        if len(value) != 2:
            raise ValueError("in: must have two arguments")
        left, right = value

        left = self.get_expression(
            left, style, params, allow_subquery=allow_subquery, depth=0
        )
        if isinstance(right, list):
            # X in (A, B, ...)
            subs = [
                self.get_expression(
                    r, style, params, allow_subquery=allow_subquery, depth=0
                )
                for r in right
            ]
            subs = self.combine(subs, separator=", ")
            return f"{indent}{left} IN ({subs})"
        else:
            # X IN (SELECT ...)
            sub = self.get_expression(
                right, style, params, allow_subquery=allow_subquery, depth=depth + 1
            )
            return f"{indent}{left} IN (\n{subs}\n{indent})"

    def get_case_expression(
        self, cases, style, params, allow_subquery=True, depth=0, indent=False
    ):
        num = len(cases)
        whens = []
        else_ = None
        if num < 2:
            raise ValueError("case: must have at least two cases")
        for i, case in enumerate(cases):
            if i == num - 1:
                # last case
                if case.get("else"):
                    # it may be either "else" or another "when"/"then" case
                    else_ = case["else"]
                    else_ = self.get_expression(
                        else_,
                        style,
                        params,
                        allow_subquery=allow_subquery,
                        depth=depth,
                        indent=indent,
                    )
                    else_ = f" ELSE {else_}"
            if not else_:
                when = case.get("when")
                then = case.get("then")
                if not when or not then:
                    raise ValueError("case: must have when and then")
                when = self.get_expression(
                    when,
                    style,
                    params,
                    allow_subquery=allow_subquery,
                    depth=depth,
                    indent=indent,
                )
                then = self.get_expression(
                    then,
                    style,
                    params,
                    allow_subquery=allow_subquery,
                    depth=depth,
                    indent=indent,
                )
                whens.append(f"WHEN {when} THEN {then}")
        whens = self.combine(whens, separator=" ")
        if not else_:
            else_ = ""
        return f"CASE {whens}{else_} END"

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
        indent = self.get_indent(depth if indent else 0)
        if isinstance(expression, (int, float, bool)):
            # literal, cast to string
            result = str(expression)
            return f"{indent}{result}"

        if expression is None:
            return f"{indent}NULL"

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
                # if quotes with ' or " or ` assume this is a literal
                char = expression[0]
                result = expression[1:-1]
                if expression[0] == self.RAW_QUOTE_CHARACTER:
                    # add inline
                    result = self.escape_literal(result)
                else:
                    # add as parameter
                    result = self.add_parameter(result, style, params)
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
            key = keys[0]
            value = expression[key]
            key = key.lower()

            if value is None:
                # keyword expression, e.g. {"default": null} -> DEFAULT (in a VAULES statement)
                if not self.validate_keyword(key):
                    raise ValueError(f'"{key}" is not a valid keyword')
                key = key.upper()
                return f"{indent}{key}"
            else:
                if self.is_command(key):
                    # subquery expression, e.g. {"select": "..."}
                    if not allow_subquery:
                        raise ValueError(
                            f'cannot build "{key}", subqueries not allowed in this expression'
                        )
                    subquery = self.get_subquery(expression, style, params, depth=depth)
                    return f"{indent}{subquery}"

                # operator/function remapping
                if key == "contains" or key == "icontains":
                    # (i)contains -> (i)like
                    key = "like" if key == "contains" else "ilike"
                    assert len(value) == 2
                    value1 = value[1]
                    if (
                        isinstance(value1, str)
                        and value1
                        and value1[0] in self.QUOTE_CHARACTERS
                    ):
                        # literal
                        quote = value1[0]
                        new_value = value1[1:-1]
                        new_value = f"{quote}%{new_value}%{quote}"
                        value[1] = new_value
                    else:
                        # identifier
                        value[1] = {"concat": ["%", value1, "%"]}
                    # {"contains": ["a", "'c'"]}
                    # -> {"like": ["a", "'%c%'"]}

                operator = self.get_operator(key)

                if operator:
                    # operator expression, e.g. {"+": [1, 2]} -> 1 + 2
                    if not isinstance(value, list):
                        # dict -> [dict]
                        value = [value]

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
                    result = self.get_case_expression(
                        value,
                        style,
                        params,
                        allow_subquery=allow_subquery,
                        depth=depth,
                    )
                    return f"{indent}{result}"
                if key == "between":
                    result = self.get_between_expression(
                        value,
                        style,
                        params,
                        allow_subquery=allow_subquery,
                        depth=depth,
                    )
                    return f"{indent}{result}"
                if key == "in":
                    result = self.get_in_expression(
                        value,
                        style,
                        params,
                        allow_subquery=allow_subquery,
                        depth=depth,
                    )
                    return result
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

                newline = newline_indent = ""
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
                        if isinstance(value, dict) and self.is_command(
                            list(value.keys())[0]
                        ):
                            newline = "\n"
                            newline_indent = self.get_indent(depth)
                            newline_indent = f"\n{newline_indent}"

                        arguments = self.get_expression(
                            value,
                            style,
                            params,
                            indent=False,
                            allow_subquery=allow_subquery,
                            depth=depth,
                        )
                return f"{indent}{key}({newline}{arguments}{newline_indent})"

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
        indent = self.get_indent(depth)

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
        type = type.upper()  # cosmetic
        return f"{indent}{name} {type}{check}{columns}{related} {deferrable} {deferred}"

    def get_alter_constraint(
        self,
        constraint: dict,
        style: ParameterStyle,
        params: Union[dict, list],
        depth: int = 0,
    ) -> str:
        indent = self.get_indent(depth)
        name = constraint.get("name")
        if not name:
            raise ValueError(f'alter constraint: "name" is required')

        changes = {}
        if "deferrable" in constraint:
            changes["deferrable"] = constraint["deferrable"]
        if "deferred" in constraint:
            changes["deferred"] = constraint["deferred"]
        if "rename" in constraint:
            if changes:
                first = next(iter(changes.keys()))
                raise ValueError(
                    f'alter constraint: cannot pass "rename" when also changing: "{first}"'
                )
            changes["name"] = constraint["rename"]

        if not changes:
            return None

        result = []
        name = self.format_identifier(name)
        if "deferrable" in changes:
            deferrable = bool(changes["deferrable"])
            deferrable = "DEFERRABLE" if deferrable else "NOT DEFERRABLE"
            result.append(f"ALTER CONSTRAINT {name} {deferrable}")
        if "deferred" in changes:
            deferred = bool(changes["deferred"])
            deferred = "INITIALLY DEFERRED" if deferred else "INITIALLY IMMEDIATE"
            result.append(f"ALTER CONSTRAINT {name} {deferred}")
        if "rename" in changes:
            new_name = self.format_identifier(changes["rename"])
            result.append(f"RENAME CONSTRAINT {name} TO {new_name}")
        result = self.combine(result, separator=", ")
        return f"{indent}{result}"

    def get_alter_column(
        self,
        column: dict,
        style: ParameterStyle,
        params: Union[dict, list],
        depth: int = 0,
    ) -> str:
        indent = self.get_indent(depth)
        name = column.get("name")
        if not name:
            raise ValueError(f'alter column: "name" is required')
        changes = {}
        if "type" in column:
            changes["type"] = column["type"]
        if "default" in column:
            changes["default"] = column["default"]
        if "null" in column:
            changes["null"] = column["null"]
        if "rename" in column:
            if changes:
                first = list(changes.keys())[0]
                raise ValueError(
                    f'alter column: cannot pass "rename" when also changing: "{first}"'
                )
            changes["name"] = column["rename"]

        if not changes:
            return None

        result = []
        name = self.format_identifier(name)
        if "type" in changes:
            type = changes["type"]
            if not self.validate_type(type):
                raise ValueError(f'alter column: invalid type "{type}"')
            result.append(f'ALTER COLUMN {name} TYPE {changes["type"]}')
        if "default" in changes:
            default = changes["default"]
            if default is not None:
                action = "SET"
                default = self.get_expression(
                    default, style, params, indent=False, allow_subquery=False
                )
                default = f" {default}"
            else:
                action = "DROP"
                default = ""
            result.append(f"ALTER COLUMN {name} {action} DEFAULT{default}")
        if "null" in changes:
            null = changes["null"]
            if null:
                action = "DROP"
            else:
                action = "SET"
            result.append(f"ALTER COLUMN {name} {action} NOT NULL")
        if "name" in changes:
            new_name = self.format_identifier(changes["name"])
            result.append(f"RENAME COLUMN {name} TO {new_name}")
        result = self.combine(result, separator=", ")
        return f"{indent}{result}"

    def get_create_column(
        self,
        column: dict,
        style: ParameterStyle,
        params: Union[dict, list],
        depth: int = 0,
    ) -> str:
        # name and type are required
        indent = self.get_indent(depth)
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
        indent = self.get_indent(depth)
        for c in columns:
            c = self.get_create_column(c, style, params, depth=0)
            items.append(f"{indent}{c}")
        if constraints:
            for c in constraints:
                c = self.get_create_constraint(c, style, params, depth=0)
                items.append(f"{indent}CONSTRAINT {c}")

        return self.combine(items, separator=",\n")
