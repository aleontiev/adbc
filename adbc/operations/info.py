from collections import OrderedDict
from asyncio import gather


class WithInfo(object):
    async def get_info(
        self,
        scope=None,
        data=True,
        schema=True,
        hashes=False,
        exclude=None,
    ):
        result = OrderedDict()
        children = await self.get_children(scope=scope)
        for child in children:
            result[child.alias] = child.get_info(
                data=data, schema=schema, hashes=hashes, exclude=exclude
            )

        keys, values = result.keys(), result.values()
        values = await gather(*values)
        return dict(zip(keys, values))
