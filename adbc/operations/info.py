from collections import OrderedDict
from asyncio import gather


class WithInfo(object):
    async def get_info(
        self, scope=None, data=True, schema=True, refresh=False
    ):
        result = OrderedDict()
        async for child in self.get_children(scope=scope, refresh=refresh):
            result[child.alias] = child.get_info(
                data=data, schema=schema, refresh=refresh
            )

        keys, values = result.keys(), result.values()
        values = await gather(*values)
        return dict(zip(keys, values))
