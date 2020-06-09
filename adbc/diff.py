from copy import deepcopy
from asyncio import gather
from jsondiff import diff


class WithDiff(object):
    async def diff(self, other, translate=None, only=None, info=False, refresh=False):
        self.log(f"{self}: diff")
        if only:
            assert only == "schema" or only == "data"

        data = self.get_info(only=only, refresh=refresh)
        other_data = other.get_info(only=only, refresh=refresh)
        data, other_data = await gather(data, other_data)
        original_data = data
        if translate:
            if info:
                original_data = deepcopy(data)
            # translate after both diffs have already been captured
            schemas = translate.get("schemas", {})
            types = translate.get("types", {})
            # table/schema names
            for key, value in schemas.items():
                if key == value:
                    continue

                # source schema "key" is the same as target schema "value"
                if key in data:
                    data[value] = data[key]
                    data.pop(key)
            # column typesa
            if types:
                types = {k: v for k, v in types.items() if k != v}
            if types:
                # iterate over all columns and change type as appropriate
                for tables in data.values():
                    for table in tables.values():
                        if "schema" not in table:
                            continue
                        for column in table["schema"]["columns"].values():
                            if column["type"] in types:
                                column["type"] = types[column["type"]]

        diff_data = diff(data, other_data, syntax="symmetric")
        return (original_data, other_data, diff_data) if info else diff_data
