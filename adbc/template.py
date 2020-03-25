import re
import copy
from adbc.utils import get


FORMAT_STRING_REGEX = re.compile('\\{\\{\\s*([^}{]+)\\s*\\}\\}')


def get_context_variables(value):
    """Get context variables inside string value"""
    return [match.group(1).strip() for match in FORMAT_STRING_REGEX.finditer(value)]


def resolve_template(value, context=None, null=Exception):
    if not value:
        return value

    if '{{' in value:
        if not context:
            raise Exception(f'must have context to resolve {value}')
    else:
        return value

    # find matches in the string
    # build new string result consisting of segments
    result = []
    results = None
    read = 0
    value_len = len(value)
    for match in FORMAT_STRING_REGEX.finditer(value):
        start = match.start()
        end = match.end()
        path = match.group(1).strip()
        null = Exception
        if path.endswith('?'):
            null = ''
            path = path[:-1]
        replace = get(context, path, null=null)

        if read < start:
            if results:
                for result in results:
                    result.append(value[read:start])
            else:
                result.append(value[read:start])

        read = end
        if isinstance(replace, list):
            if result:
                for result in results:
                    for rep in replace:
                        result.append(rep)
            else:
                results = copy.copy(replace)
                for i, res in enumerate(results):
                    results[i] = replace + res
        else:
            if results:
                for result in results:
                    result.append(replace)
            else:
                result.append(replace)

    if read < value_len:
        if results:
            for result in results:
                result.append(value[read:value_len])
        else:
            result.append(value[read:value_len])

    if results:
        return [''.join(result) for result in results]
    else:
        return ''.join(result) if result else value
