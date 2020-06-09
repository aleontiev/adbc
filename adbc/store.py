from asyncio import gather
from collections import OrderedDict
from fnmatch import fnmatch

from .logging import Loggable
from .utils import cached_property, merge
from .exceptions import NotIncluded


def specificity(item):
    index, (key, value) = item
    wildcards = key.count('*')
    inverse = 1 if key.startswith('~') else 0
    others = len(key) - wildcards - inverse
    return (0 if wildcards > 0 else 1, wildcards, others, index)


class Store(Loggable):
    async def push(self, other):
        raise NotImplementedError()

    async def pull(self, other):
        raise NotImplementedError()

    async def get_count(self):
        raise NotImplementedError()


class WithChildren(object):
    async def get_children(self, **kwargs):
        raise NotImplementedError()

    async def get_count(self, **kwargs):
        tasks = []
        refresh = kwargs.pop('refresh', False)
        async for child in self.get_children(refresh=refresh):
            # get counts in parallel
            tasks.append(child.get_count())

        return sum(await gather(*tasks))

    async def get_info(self, only=None, refresh=False):
        data = OrderedDict()
        async for child in self.get_children(refresh=refresh):
            data[child.name] = child.get_info(only=only, refresh=refresh)

        keys, values = data.keys(), data.values()
        values = await gather(*values)
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

        if (
            not config or (
                isinstance(config, dict) and not config.get('enabled', True)
            )
        ):
            raise NotIncluded()

        return config


class ParentStore(WithChildren, Store):
    pass
