from collections import defaultdict
from typing import Union, List
from .sql import SQLBuilder


class PostgresBuilder(SQLBuilder):
    IDENTIFIER_QUOTE_CHARACTER = '"'
    LITERAL_QUOTE_CHARACTER = "'"
    FUNCTION_RENAMES = {
        'json_array': 'json_build_array'
    }
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
        for type, clauses in sorted(by_other.items(), key=lambda x: self.SCHEMA_ORDER[x[0]]):
            # in order: database, schema, table, ..., column, constraint
            if type == "table":
                clauses = list(by_table.values())
                if len(clauses) == 1:
                    clauses = clauses[0]
            normalized.append({type: clauses})

        for type, clauses in sorted(
            renames.items(), key=lambda x: -1 * self.SCHEMA_ORDER[x[0]]
        ):
            # in order: constraint, column,..., table, schema, database
            normalized.append({type: clauses})
        if len(normalized) == 1:
            normalized = normalized[0]
        return {"alter": normalized}

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
