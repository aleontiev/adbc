import re
from adbc.template import resolve_template, get_context_variables


IDENTIFIER_REGEX = re.compile("^[a-zA-Z][-_a-zA-Z0-9$]*$")

RAW_EXPRESSION_WHITELIST = {
    'NOW()',
    'DEFAULT'
}


def should_escape(value):
    if not getattr(value, '_raw', None):
        return True
    return str(value.upper()) not in RAW_EXPRESSION_WHITELIST


class Raw(str):
    _raw = True

    def __copy__(self):
        return Raw(str(self))


def quote(ident):
    return f'"{ident}"'


def format_table(table, schema=None, check=True):
    if check:
        check_identifier(table)
        if schema:
            check_identifier(schema)
    return f'{quote(schema)}.{quote(table)}' if schema else quote(table)


def format_column(col, check=True, table=None, schema=None):
    if check:
        check_identifier(col)
    if table:
        table = format_table(table, schema, check=check)

    return f'{table}.{quote(col)}' if table else quote(col)


def check_identifier(ident):
    if not IDENTIFIER_REGEX.match(ident):
        raise Exception(f'invalid identifier name: {ident}')
    return ident


def sort_columns(cols, check=True, table=None, schema=None):
    columns = []
    for c in cols:
        direction = "ASC"
        if c.startswith("-"):
            direction = "DESC"
            c = c[1:]
        columns.append(f'{format_column(c, check, table, schema)} {direction}')
    return ", ".join(columns)


def list_columns(cols, check=True, table=None, schema=None, aliases=None):
    return ", ".join([
        format_column(c, check=check, table=table, schema=schema) for c in cols
    ])


def parens(value):
    return f"({value})"


OPERATORS = {
    "equal": '"{{ field }}" = {{ value }}',
    "less": '"{{ field }}" < {{ value }}',
    "at.most": '"{{ field }}" <= {{ value }}',
    "greater": '"{{ field }}" > {{ value }}',
    "at.least": '"{{ field }}" >= {{ value }}',
    "like": '"{{ field }}" LIKE {{ value }}',
    "ilike": '"{{ field }}" ILIKE {{ value }}',
    "not.equal": '"{{ field }}" != {{ value }}',
    "is.null": '"{{ field }}" IS {{ not }}NULL',
    "starts.with": '"{{ field }}" LIKE {{ value }}',
    "ends.with": '"{{ field }}" LIKE {{ value }}',
    "contains": '"{{ field }}" LIKE {{ value }}',
    "icontains": '"{{ field }}" ILIKE {{ value }}',
    "in": '"{{ field }}" IN {{ value }}',
    'not.equal': '"{{ field }}" != {{ value }}',
}


OPERATOR_TRANSLATE = {
    "starts.with": "%{{ value }}",
    "ends.with": "{{ value }}%",
    "contains": "%{{ value }}%",
    "icontains": "%{{ value }}%",
}


OPERATORS['ne'] = OPERATORS['!='] = OPERATORS['<>'] = OPERATORS['not.equal']
OPERATORS["eq"] = OPERATORS["equals"] = OPERATORS["="] = OPERATORS["equal"]
OPERATORS["less.than"] = OPERATORS["<"] = OPERATORS["less"]
OPERATORS["greater.than"] = OPERATORS[">"] = OPERATORS["greater"]
OPERATORS["greater.equal"] = OPERATORS[">="] = OPERATORS["at.least"]
OPERATORS["less.equal"] = OPERATORS["<="] = OPERATORS["at.most"]
OPERATORS["~"] = OPERATORS["like"]
OPERATORS["~~"] = OPERATORS["ilike"]


def escape_like(like):
    like = like.replace("\\", "\\\\")
    like = like.replace("%", "\\%")
    like = like.replace("_", "\\_")
    return like


def params_list(start, num):
    return [f"${start + i}" for i in range(num)]


def where_clause(where, args):
    ands = where.get(".and")
    ors = where.get(".or")
    nots = where.get(".not")

    if ands:
        return " AND ".join([parens(where_clause(a, args)) for a in ands])
    elif ors:
        return " OR ".join([parens(where_clause(o, args)) for o in ors])
    elif nots:
        return f"NOT ({where_clause(nots, args)})"

    clauses = []
    for field, operator_value in where.items():
        context = {"field": field}

        if isinstance(operator_value, dict):
            assert operator_value
        else:
            operator_value = {'equals': operator_value}

        clause = []
        for operator, value in operator_value.items():
            template = OPERATORS.get(operator)
            assert template

            like = "LIKE" in template.upper()
            needs = get_context_variables(template)
            # escape

            if like:
                value = escape_like(value)

            # translate, e.g. add wildcards for LIKE operators
            translate = OPERATOR_TRANSLATE.get(operator)
            if translate:
                value = resolve_template(translate, {'value': value})

            if "not" in needs:
                # add NOT for IS
                context["not"] = "" if bool(value) else "NOT "

            context['value'] = value
            # replace value with parameters
            for key in needs:
                if key == "value":
                    if isinstance(value, (list, tuple)):
                        num_values = len(value)
                        num_args = len(args)
                        args.extend(value)
                        params = params_list(num_args+1, num_values)
                        context["value"] = f"({', '.join(params)})"
                    else:
                        args.append(value)
                        context["value"] = f"${len(args)}"

            clause.append(resolve_template(template, context))
        if len(clause) > 1:
            clauses.append(' AND '.join([parens(c) for c in clause]))
        else:
            clauses.append(clause[0])

    num = len(clauses)
    if num > 1:
        return " AND ".join([parens(c) for c in clauses])
    elif num:
        return clauses[0]
    else:
        return ''


def print_query(query, sep='\n-----\n'):
    query, *args = query
    if not args:
        return query
    else:
        args = '\n'.join([f'${i+1}: {a}' for i, a in enumerate(args)])
        return f'{query}{sep}{args}'
