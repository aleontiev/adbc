import re
import inspect
import asyncio
import collections
from ddlparse import DdlParse
from cached_property import cached_property  # noqa


def get_include_args(args, truth=True):
    """Get command args that in/exclude based on a value"""
    include = {}
    if isinstance(args, str):
        args = args.split(",")

    for arg in args:
        if arg.startswith("~") or arg.startswith("-"):
            arg = arg[1:]
            include[arg] = False
        else:
            include[arg] = truth
    return include


SERVER_VERSION_NUMBER_REGEX = re.compile("^[A-Za-z]+ ([0-9.]+)")


def get_version_number(version):
    match = SERVER_VERSION_NUMBER_REGEX.match(version)
    if match:
        return float(".".join(match.group(1).split(".")[0:2]))
    else:
        raise Exception("Not a valid server version string")


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
    allow_null = not (inspect.isclass(null) and issubclass(null, Exception))
    for part in parts:
        if context is None:
            if allow_null:
                return null
            else:
                raise null(f'context is null but next part: "{part}"')
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
                    raise null(f"context index out of bounds: {part}")
        else:
            try:
                context = getattr(context, part)
            except AttributeError:
                if allow_null:
                    return null
                else:
                    raise null(f'could not resolve context on part "{part}"')

    if context and callable(context):
        # if the result is a callable,
        # try to resolve it
        context = context()
    return context


def merge(dictionary, other):
    for k, v in other.items():
        if isinstance(v, collections.abc.Mapping):
            dictionary[k] = merge(dictionary.get(k, {}), v)
        else:
            dictionary[k] = v
    return dictionary


def confirm(prompt, default=False):
    if default:
        prompt = f"{prompt} ([y] / n): "
    else:
        prompt = f"{prompt} ([n] / y): "
    while True:
        ans = input(prompt).strip().replace("\n", "").lower()
        if not ans:
            return default
        if ans not in ["y", "n", "yes", "no"]:
            print("Please enter y or n, or hit enter: ")
            continue
        if ans in {"y", "yes"}:
            return True
        if ans in {"n", "no"}:
            return False


def get_first(items, fn, then=None):
    if isinstance(items, dict):
        items = items.values()

    for item in items:
        if fn(item):
            return item[then] if then else item
    return None


def split_field(i, f):
    for key in i:
        value = key.pop(f, None)
        yield (value, key)


class AsyncContext(object):
    def __init__(self, args=None):
        self.args = args

    async def __aenter__(self):
        return self.args

    async def __aexit__(self, *args):
        pass


aecho = AsyncContext


class AsyncBuffer(object):
    DEBUFFER = 100

    def __init__(self, debug=False):
        self._debug = debug
        self._buffer = []
        self._read = -1
        self._reads = 0
        self._waits = 0
        self._writes = 0
        self._waiting = 0
        self._buffmax = 0
        self._waiter = None
        self._closed = False

    async def write(self, data):
        if self._debug:
            print('write buffer:', data)
        self._writes += 1
        self._buffer.append(data)
        self._buffmax = max(self._buffmax, len(self._buffer))
        if self._waiting and self._waiter:
            self._waiter.set_result(data)

    def close(self):
        # no more writes, but can still read out the rest
        self._closed = True
        if self._waiter:
            self._waiter.cancel()

    def __aiter__(self):
        return self

    def debuffer(self):
        if self._read > 0 and self._read % self.DEBUFFER == 0:
            # debuffer any rows not yet read
            # and reset read counter to 0
            self._buffer = self._buffer[self._read + 1:]
            self._read = -1

    async def wait(self):
        self._waits += 1
        self._waiting += 1
        self._waiter = asyncio.get_running_loop().create_future()
        await self._waiter
        self._waiter = None
        self._waiting -= 1

    async def __anext__(self):
        index = self._read + 1
        if index >= len(self._buffer):
            if self._closed:
                if self._debug:
                    print(
                        'closed buffer, '
                        f"reads: {self._reads}, "
                        f"writes: {self._writes}, "
                        f"waits: {self._waits}, "
                        f"buffmax: {self._buffmax}"
                    )
                raise StopAsyncIteration()

            await self.wait()

        result = self._buffer[index]
        self._read = index
        self.debuffer()
        self._reads += 1
        if self._debug:
            print('read buffer: ', result)
        return result

def flatten(x):
    return [a for b in x for a in b]


def raise_not_implemented(message):
    def inner(*args, **kwargs):
        raise NotImplementedError(message)
    return inner


def print_query(query, params, sep='\n-----\n'):
    if not params:
        return query
    else:
        args = '\n'.join([f'${i+1}: {a}' for i, a in enumerate(params)])
        return f'{query}{sep}{args}'


def parse_create_table(statement: str):
    """useful for sqlite DDL parsing"""
    columns = []
    constraints = []
    indexes = []
    parsed = DdlParse().parse(statement)
    for name, column in parsed.columns.items():
        column_ = {}
        column_['type'] = column.data_type
        column_['null'] = not column.not_null
        # TODO: support parsing default from SQL to zQL
        column_['default'] = column.default
        column_['primary'] = column.primary_key
        column_['unique'] = column.unique
        column_['sequence'] = column.auto_increment
        column_['name'] = name
        columns.append(column_)
        # TODO: support foreign keys
        # TODO: support indexes
    result = columns, constraints, indexes
    print('parse create table', result)
    return result
