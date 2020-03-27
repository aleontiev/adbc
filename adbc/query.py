from .utils import merge as _merge
from copy import deepcopy
from .exceptions import QueryValidationError, QueryExecutionError


class NestedFeature(object):
    """Helper class for Query"""

    def __init__(self, query, name, level=None):
        self.query = query
        self.name = name
        self.level = level

    def __getattr__(self, key):
        # adjust level
        if self.level:
            level = "{}.{}".format(self.level, key)
        else:
            level = key
        return NestedFeature(query=self.query, name=self.name, level=level)

    def __call__(self, *args, **kwargs):
        args = [self.level] + list(args)
        # call back to query with arguments
        return getattr(self.query, "_{}".format(self.name))(*args, **kwargs)


class Query(object):
    # methods
    def __init__(self, table=None, state=None, executor=None):
        """
        Arguments:
            state: internal query representation
        """
        self._state = state or {}
        self.table = table
        self.executor = executor

    def get_state(self, level=None):
        state = self.state
        if level:
            for l in level.split("."):
                state = state[l]
        return state

    def data(self, key, level=None, default=None):
        return self.get_state(level).get(f".{key}", default)

    def all_columns(self, level=None):
        table = self.table
        return list(table.columns.keys())

    def columns(self, level=None):
        result = set()
        state = self.get_state(level)
        if '*' in state:
            value = state['*']
            all_columns = set(self.all_columns(level))
            if value:
                result |= all_columns
            else:
                result -= all_columns
        remove = set()
        for k, v in state.items():
            if k.startswith(".") or k == '*':
                continue
            if v:
                result.add(k)
            else:
                remove.add(k)

        if not result and self.data('method') in ('get', 'one'):
            # automatic * for get/one
            result = self.all_columns(level)

        for k in remove:
            if k in result:
                result.remove(k)

        return list(sorted(result))

    async def count(self, **kwargs):
        return await self._call("count")

    # INSERT
    async def add(self, key=None, field=None):
        return await self._call("add", key=key, field=field)

    # UPDATE
    async def set(self, key=None, field=None):
        return await self._call("set", key=key, field=field)

    # SELECT
    async def get(self, key=None, field=None):
        return await self._call("get", key=key, field=field)

    # SELECT
    async def one(self, key=None, field=None):
        return await self._call("one", key=key, field=field)

    # DELETE
    async def delete(self, key=None, field=None):
        return await self._call("delete", key=key, field=field)

    async def execute(self, **kwargs):
        executor = self.executor
        if not executor:
            raise QueryExecutionError(f"Query cannot execute without executor")
        method_name = self.data("method")
        method = getattr(self.executor, method_name, None)
        if not method:
            raise QueryValidationError(f"Invalid method {method_name}")
        return await method(self, **kwargs)

    @property
    def state(self):
        return self._state

    def key(self, name):
        return self._update({".key": name})

    def field(self, name):
        return self._update({".field": name})

    def method(self, name):
        return self._update({".method": name})

    def body(self, body):
        return self._update({".body": body}, merge=True)

    def limit(self, limit):
        return self._update({".limit": limit})

    @property
    def take(self):
        return NestedFeature(self, "take")

    @property
    def where(self):
        return NestedFeature(self, "where")

    @property
    def sort(self):
        return NestedFeature(self, "sort")

    def validate_field(self, level, field):
        return True

    def _take(self, level, *args, copy=True):
        kwargs = {}
        for arg in args:
            take = True
            if arg.startswith("-"):
                arg = arg[1:]
                take = False
            self.validate_field(level, arg)
            kwargs[arg] = take
        return self._update(kwargs, copy=copy, level=level, merge=True)

    async def _call(self, method, key=None, field=None):
        if self.data("method") != method:
            return await getattr(self.method(method), method)(key=key, field=field)

        if key or field:
            # redirect back through copy
            args = {}
            if key:
                args[".key"] = key
            if field:
                args[".field"] = field
            return await getattr(self._update(args), method)()

        return await self.execute()

    def validate_where(self, level, query):
        return True

    def validate_sort(self, level, query):
        return True

    def _where(self, level, query, copy=True):
        """
        Example:
            .where({
                '.or': [
                    {'users.location.name': {'contains': 'New York'}},
                    {'.not': {'users.in': [1, 2]}}
                ]
            })
        """
        self.validate_where(level, query)
        return self._update({".where": query}, copy=copy, level=level)

    def _sort(self, level, *args, copy=True):
        """
        Example:
            .sort("name", "-created")
        """
        self.validate_sort(level, args)
        return self._update({".sort": args}, copy=copy, level=level)

    def __str__(self):
        return str(self.state)

    def _update(self, args=None, level=None, merge=False, copy=True, **kwargs):
        if args:
            kwargs = args

        state = None
        if copy:
            state = deepcopy(self.state)
        else:
            state = self.state

        sub = state
        # adjust substate at particular level
        # default: adjust root level
        if level:
            for part in level.split("."):
                try:
                    new_sub = sub[part]
                except KeyError:
                    sub[part] = {}
                    sub = sub[part]
                else:
                    if isinstance(new_sub, bool):
                        sub[part] = {}
                        sub = sub[part]
                    else:
                        sub = new_sub

        for key, value in kwargs.items():
            if merge and isinstance(value, dict) and sub.get(key):
                # deep merge
                _merge(value, sub[key])
            else:
                # shallow merge, assign the state
                sub[key] = value

        if copy:
            return Query(table=self.table, state=state, executor=self.executor)
        else:
            return self

    def __getitem__(self, key):
        return self._state[key]
