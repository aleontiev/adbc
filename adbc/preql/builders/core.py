from adbc.preql.validator import Validator


class Builder(Validator):
    INDENT = 4
    IDENTIFIER_SPLIT_CHARACTER = '.'
    WHITESPACE_CHARACTER = ' '
    WILDCARD_CHARACTER = '*'
    QUOTE_CHARACTERS = {'"', "'", '`'}
    RAW_QUOTE_CHARACTER = '`'
    COMMANDS = {
        'select',
        'insert',
        'update',
        'delete',
        'truncate',
        'create',
        'alter',
        'drop',
        'show',
        'explain',
        'set'
    }
    OPERATORS = {
        'not': 1,
        '!!': 1,
        'is null': {
            'arguments': 1,
            'binds': 'right'
        },
        'is not null': {
            'arguments': 1,
            'binds': 'right'
        },
        '!': {
            'arguments': 1,
            'binds': 'right'
        },
        '@': 1,
        '|/': 1,
        '=': 2,
        '+': 2,
        '*': 2,
        '-': 2,
        '/': 2,
        '%': 2,
        '^': 2,
        '#': 2,
        '~': 1,
        '>>': 2,
        '&': 2,
        '<<': 2,
        '|': 2,
        '||': 2,
        '<': 2,
        '<=': 2,
        '-': 2,
        '!=': 2,
        '<>': 2,
        'like': 2,
        'ilike': 2,
        '~~': 2,
        '!~~': 2,
        '>': 2,
        '>=': 2,
        'and': 2,
        'or': 2,
    }
    # TODO: handle non-functional clause expressions
    # like CASE, BETWEEN, etc
    CLAUSES = {
        'case',
        'between'
    }
