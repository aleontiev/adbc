from collections import OrderedDict
from hashlib import md5
from fnmatch import fnmatch
from .utils import cached_property, merge
import asyncio


def hash_(s, n):
    return md5(
        ",".join(["{}-{}".format(s[i], n[i]) for i in range(len(s))]).encode("utf-8")
    ).hexdigest()


def specificity(item):
    index, (key, value) = item
    wildcards = key.count('*')
    inverse = 1 if key.startswith('~') else 0
    others = len(key) - wildcards - inverse
    return (0 if wildcards > 0 else 1, wildcards, others, index)


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
        return hash_(s, n)

    async def get_schema_hash(self):
        s = []
        n = []
        async for c in self.get_children():
            # get schema hashes in parallel
            schema_hash = c.get_schema_hash()
            s.append(schema_hash)
            n.append(c.name)

        s = await asyncio.gather(*s)
        return hash_(s, n)

    async def get_diff_data(self):
        self.log("{}.{}.diff".format(self.type, self.name))
        data = OrderedDict()
        async for child in self.get_children():
            data[child.name] = child.get_diff_data()

        keys, values = data.keys(), data.values()
        values = await asyncio.gather(*values)
        return dict(zip(keys, values))


class WithConfig(object):
    def get_child_include(self):
        config = self.config
        if config is True or config is None:
            return True

        return config.get(self.child_key, True)

    @cached_property
    def _sorted_child_configs(self):
        configs = self.get_child_include()
        if configs is True:
            return {}

        configs = list(enumerate(configs.items()))
        configs.sort(key=specificity)
        return [config[1] for config in configs]

    def get_child_config(self, name):
        include = self.get_child_include()

        if include is True:
            # empty configuration (all included)
            return True

        config = {}

        # merge all matching configuration entries
        # go in order from least specific to most specific
        # this means exact-match config will take highest precedence
        for key, child in self._sorted_child_configs:
            inverse = False
            if key.startswith('~'):
                inverse = True
                key = key[1:]
            match = fnmatch(name, key)
            if (match and not inverse) or (not match and inverse):
                # we have a match, merge in the config
                if child is False:
                    child = {'enabled': False}
                elif child is True:
                    child = {'enabled': True}
                merge(config, child)

        if not config:
            return True

        return config


class ParentStore(WithChildren, Store):
    pass
