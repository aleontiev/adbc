import json


class WithCache():
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.reset()

    def get_cache_key(self, key):
        return json.dumps(key)

    def cache_by(self, primary, secondary, method):
        cache = self._get_secondary(primary)
        secondary = self.get_cache_key(secondary)

        if secondary not in cache:
            cache[secondary] = method()

        return cache[secondary]

    def _get_secondary(self, primary):
        if not hasattr(self, '_cache'):
            self.reset()

        primary = self.get_cache_key(primary)

        if primary not in self._cache:
            self._cache[primary] = {}

        return self._cache[primary]

    async def cache_by_async(self, primary, secondary, method):
        cache = self._get_secondary(primary)
        secondary = self.get_cache_key(secondary)

        if secondary not in cache:
            cache[secondary] = await method()

        return cache[secondary]

    def reset(self):
        self._cache = {}
