import re
import inspect


def get_include_args(args, truth=True):
    """Get command args that in/exclude based on a value"""
    include = {}
    if isinstance(args, str):
        args = args.split(',')

    for arg in args:
        if arg.startswith('~') or arg.startswith('-'):
            arg = arg[1:]
            include[arg] = False
        else:
            include[arg] = truth
    return include


def get_include_query(
    include,
    table,
    column,
):
    """Get query filters that in/exclude based on a particular column"""

    if not include or include is True:
        # no filters
        return ('', [])

    args = []
    query = []
    count = 1
    includes = excludes = False
    for key, should in include.items():
        if '*' in key:
            operator = '~~' if should else '!~~'
            key = key.replace('*', '%')
        else:
            operator = '=' if should else '!='
        args.append(key)
        query.append(
            '({}."{}" {} ${})'.format(
                table,
                column,
                operator,
                count
            )
        )
        count += 1
        if should:
            includes = True
        else:
            excludes = True

    if includes and not excludes:
        union = 'OR'
    else:
        union = 'AND'
    return ' {} '.format(union).join(query), args


SERVER_VERSION_REGEX = re.compile('^[A-Za-z]+ ([0-9.]+)')


def get_server_version(version):
    match = SERVER_VERSION_REGEX.match(version)
    if match:
        return match.group(1)
    else:
        raise Exception('Not a valid server version string')


def get(context, path, null=None):
    """Resolve a value given a path and a deeply-nested object

    Arguments:
        path: a dot-separated string
        context: any object, list, dictionary,
            or single-argument callable
        null: if Exception type, bad key raises Exception
            if other, bad key returns passed-in value
            default: None
    Returns:
        value at the end of the path, or None

    Examples:

        T = namedtuple('T', ['x'])
        context = {"a": [T('y')]}
        get(context, "a.0.x") == 'y'
    """
    parts = path.split(".")
    allow_null = not (
        inspect.isclass(null) and issubclass(null, Exception)
    )
    for part in parts:
        if context is None:
            if allow_null:
                return null
            else:
                raise null(
                    f'context is null but next part: "{part}"'
                )
        if callable(context):
            # try to "call" into the context
            try:
                try:
                    # 1. assume it is a method that takes no arguments
                    # and returns a nested object
                    context = context()
                except TypeError:
                    # 2. assume its a method that takes the next part
                    # as the argument
                    context = context(part)
                    continue
            except Exception:
                # fallback: assume this is a special object
                # that we should not call into
                # e.g. a django ManyRelatedManager
                pass

        if isinstance(context, dict):
            if part in context:
                context = context[part]
            else:
                if allow_null:
                    return null
                else:
                    raise null(f'could not resolve context on part "{part}"')
        elif isinstance(context, list):
            # throws ValueError if part is NaN
            part = int(part)
            try:
                context = context[part]
            except IndexError:
                if allow_null:
                    return null
                else:
                    raise null(f'context index out of bounds: {part}')
        else:
            if hasattr(context, part):
                context = getattr(context, part)
            else:
                if allow_null:
                    return null
                else:
                    raise null(f'could not resolve context on part "{part}"')

    if context and callable(context):
        # if the result is a callable,
        # try to resolve it
        context = context()
    return context


def is_dsn(url):
    from asyncpg.connect_utils import _parse_connect_dsn_and_args
    try:
        _parse_connect_dsn_and_args(url)
    except Exception:
        return False
