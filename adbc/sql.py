"""
TODO: deprecate and replace this with backend-agnostic SQLFormatter
"""
import re


TAGGED_NUMBER_REGEX = re.compile(r'[a-zA-Z]+ ([0-9]+)')


def get_tagged_number(value):
    match = TAGGED_NUMBER_REGEX.match(value)
    if not match:
        raise Exception('not a tagged number: {value}')

    return int(match.group(1))


def print_query(query, params, sep='\n-----\n'):
    if not params:
        return query
    else:
        args = '\n'.join([f'${i+1}: {a}' for i, a in enumerate(params)])
        return f'{query}{sep}{args}'
