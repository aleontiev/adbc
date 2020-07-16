from collections import OrderedDict
from asyncio import gather


class WithInfo(object):
    async def get_info(
        self, scope=None, data=True, schema=True, refresh=False, hashes=False
    ):
        last_scope = getattr(self, '_last_scope', self.scope)
        if last_scope != scope:
            # force refresh if running get_info
            # with different scope
            refresh = True

        result = OrderedDict()

        if refresh:
            self.clear_cache()

        async for child in self.get_children(scope=scope):
            result[child.alias] = child.get_info(
                data=data, schema=schema, hashes=hashes
            )

        keys, values = result.keys(), result.values()
        values = await gather(*values)
        self._last_scope = scope
        return dict(zip(keys, values))
