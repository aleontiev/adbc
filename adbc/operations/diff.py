from asyncio import gather
from jsondiff import diff
from .info import WithInfo


class WithDiff(WithInfo):
    async def diff(
        self, target, scope=None, data=True, schema=True, info=False, refresh=False
    ):
        self.log(f"{self}: diff")
        data = self.get_info(scope=scope, schema=schema, data=data, refresh=refresh)
        target_data = target.get_info(
            scope=scope, schema=schema, data=data, refresh=refresh
        )
        source_data, target_data = await gather(data, target_data)
        diff_data = diff(source_data, target_data, syntax="symmetric")
        return (source_data, target_data, diff_data) if info else diff_data
