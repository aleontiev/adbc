from asyncio import gather
from jsondiff import diff
from .info import WithInfo


class WithDiff(WithInfo):
    async def diff(
        self,
        target,
        scope=None,
        data=True,
        schema=True,
        info=False,
        refresh=False,
        hashes=False,
    ):
        self.log(f"{self}: diff")
        source_info = self.get_info(
            scope=scope, schema=schema, data=data, refresh=refresh, hashes=hashes
        )
        target_info = target.get_info(
            scope=scope, schema=schema, data=data, refresh=refresh, hashes=hashes
        )
        source_info, target_info = await gather(source_info, target_info)
        diff_info = diff(source_info, target_info, syntax="symmetric")
        return (source_info, target_info, diff_info) if info else diff_info
