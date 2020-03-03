from collections import OrderedDict
from hashlib import md5
from fnmatch import fnmatch
import asyncio


def _hash(s, n):
    return md5(
        ",".join(["{}-{}".format(s[i], n[i]) for i in range(len(s))]).encode("utf-8")
    ).hexdigest()


class Loggable(object):
    def log(self, *args, **kwargs):
        if self.verbose:
            print(*args, **kwargs)


class Store(Loggable):
    async def push(self, other):
        raise NotImplementedError()

    async def pull(self, other):
        raise NotImplementedError()

    async def get_count(self):
        raise NotImplementedError()

    async def get_data_hash(self):
        raise NotImplementedError()

    async def get_schema_hash(self):
        raise NotImplementedError()


class WithChildren(object):
    async def get_children(self):
        raise NotImplementedError()

    async def get_count(self):
        s = []
        async for c in self.get_children():
            # get counts in parallel
            s.append(c.get_count())

        return sum(await asyncio.gather(*s))

    async def get_data_hash(self):
        s = []
        n = []
        async for c in self.get_children():
            # get data hashes in parallel
            data_hash = c.get_data_hash()
            s.append(data_hash)
            n.append(c.name)

        s = await asyncio.gather(*s)
        return _hash(s, n)

    async def get_schema_hash(self):
        s = []
        n = []
        async for c in self.get_children():
            # get schema hashes in parallel
            schema_hash = c.get_schema_hash()
            s.append(schema_hash)
            n.append(c.name)

        s = await asyncio.gather(*s)
        return _hash(s, n)

    async def get_diff_data(self):
        self.log("{}.{}.diff".format(self.type, self.name))
        data = OrderedDict()
        async for child in self.get_children():
            data[child.name] = child.get_diff_data()

        keys, values = data.keys(), data.values()
        values = await asyncio.gather(*values)
        return dict(zip(keys, values))


class WithInclude(object):
    def get_include(self, name):
        include = self.include
        if include is True:
            # assumes all included
            return True

        if name in include:
            return include[name]

        else:
            for key, should in include.items():
                if "*" in key:
                    match = fnmatch(name, key)
                    if (match and should) or (not match and not should):
                        return True if not should else should

        return False


class ParentStore(WithChildren, Store):
    pass
