from jsondiff import diff
from .copy import WithCopy


class WithApply(WithCopy):
    async def apply(self, schema: dict, scope=None):
        info = await self.get_info(
            schema=True,
            data=False,
            hashes=False,
            scope=scope
        )
        diff_ = diff(schema, info, syntax="symmetric")
        changes = await self.copy_metadata(diff_, scope=scope)
        return changes
