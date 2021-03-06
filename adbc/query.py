from .utils import merge as _merge
from copy import deepcopy
from .exceptions import QueryValidationError, QueryExecutionError
from .executors import get_executor


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
    def __init__(self, database=None, state=None, executor=None, scope=None):
        """
        Arguments:
            database: Database object
            state: internal query representation$
            executor: pre-existing executor (optional)
            scope: scope with which the model was called
        """
        self._state = state or {}
        self.database = database
        self.scope = scope
        self.executor = executor or get_executor(self.database, scope)

    def get_state(self, level=None):
        state = self.state
        if level:
            for l in level.split("."):
                state = state[l]
        return state

    def data(self, key, level=None, default=None):
        return self.get_state(level).get(key, default)

    async def count(self, **kwargs):
        return await self._call("count", **kwargs)

    # INSERT
    async def add(self, key=None, field=None, **kwargs):
        return await self._call("add", key=key, field=field, **kwargs)

    # UPDATE
    async def set(self, key=None, field=None, **kwargs):
        return await self._call("set", key=key, field=field, **kwargs)

    # SELECT
    async def get(self, key=None, field=None, **kwargs):
        return await self._call("get", key=key, field=field, **kwargs)

    # SELECT
    async def one(self, key=None, field=None, **kwargs):
        return await self._call("one", key=key, field=field, **kwargs)

    # DELETE
    async def delete(self, key=None, field=None, **kwargs):
        return await self._call("delete", key=key, field=field, **kwargs)

    # TRUNCATE
    async def truncate(self, **kwargs):
        return await self._call("truncate", **kwargs)

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

    def source(self, name):
        return self._update({"source": name})

    def field(self, name):
        return self._update({"field": name})

    def key(self, name):
        return self._update({"key": name})

    def method(self, name):
        return self._update({"method": name})

    def values(self, values):
        return self._update({"values": values}, merge=True)

    def limit(self, limit):
        return self._update({"limit": limit})

    @property
    def take(self):
        return NestedFeature(self, "take")

    @property
    def where(self):
        return NestedFeature(self, "where")

    @property
    def sort(self):
        return NestedFeature(self, "sort")

    @property
    def join(self):
        return NestedFeature(self, "join")

    def validate_field(self, level, field):
        return True

    def _join(self, level, *args, **kwargs):
        return self._update({"join": args}, level=level)

    def _take(self, level, *args, **kwargs):
        options = {}
        if args and isinstance(args[0], str):
            for arg in args:
                take = arg
                if arg.startswith("-"):
                    arg = arg[1:]
                    take = None
                self.validate_field(level, arg)
                options[arg] = take
        elif args and isinstance(args[0], dict):
            options = args[0]
        for k, v in kwargs.items():
            options[k] = v

        if not options:
            raise ValueError('take: expecting at least one argument')
        return self._update({'take': options}, level=level, merge=True)

    async def _call(self, method, key=None, field=None, **kwargs):
        if self.data("method") != method:
            return await getattr(self.method(method), method)(
                key=key, field=field, **kwargs
            )

        if key or field:
            # redirect back through copy
            args = {}
            if key:
                args["key"] = key
            if field:
                args["field"] = field
            return await getattr(self._update(args), method)(**kwargs)

        return await self.execute(**kwargs)

    def validate_where(self, level, query):
        return True

    def validate_sort(self, level, query):
        return True

    @classmethod
    def _simple_expression(cls, simple, join='and'):
        expr = []
        for key, value in simple.items():
            key = key.split('__')
            len_key = len(key)
            if len_key == 1:
                operator = '='
                key = key[0]
            elif len_key == 2:
                operator = key[-1]
                key = '__'.join(key[:-1])
            expr.append({operator: [key, value]})
        return {join: expr} if len(expr) > 1 else expr[0]

    def _where(self, *args, **kwargs):
        """
        Example:
            .where({
                'or': [
                    {'contains': {'users.location.name': "'New York'"}},
                    {'not': {'in': ['users', [1, 2]}}
                ]
            })
            .where(id=1)
        """
        query = None
        if not args:
            raise ValueError('at least one argument required')

        level = args[0]
        if len(args) > 1:
            query = args[1]

        if not query and kwargs:
            query = self._simple_expression(kwargs)

        if not query:
            raise ValueError('no query arguments defined')

        self.validate_where(level, query)
        return self._update({"where": query}, level=level)

    def _sort(self, level, *args, **kwargs):
        """
        Example:
            .sort("name", "-created")
        """
        self.validate_sort(level, args)
        return self._update({"sort": args}, level=level)

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
            return Query(
                database=self.database,
                state=state,
                executor=self.executor,
                scope=self.scope
            )
        else:
            return self

    def __getitem__(self, key):
        return self._state[key]


class TableModel(Query):
    def __init__(self, table=None, **kwargs):
        self.table = table
        if 'state' not in kwargs:
            kwargs['state'] = {
                'source': [table.namespace.name, table.name]
            }

        super().__init__(**kwargs)

    def __str__(self):
        return f'TableModel({self.table})'
