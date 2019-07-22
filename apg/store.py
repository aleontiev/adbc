from hashlib import md5

class Store(object):
    async def push(self, other):
        raise NotImplementedError()

    async def pull(self, other):
        raise NotImplementedError()

    async def get_diff(self, other):
        raise NotImplementedError()

    async def get_count(self):
        raise NotImplementedError()

    async def get_data_hash(self):
        raise NotImplementedError()

    async def get_schema_hash(self):
        raise NotImplementedError()

    async def get_signature(self):
        return f"{self.data_hash()}-{self.schema_hash()}-{self.count()}"


class WithChildren(object):
    async def get_children(self):
        raise NotImplemented()

    async def get_count(self):
        return sum([c.get_count() for c in self.get_children()])

    async def get_data_hash(self):
        return md5(
            ','.join([
                '{}-{}'.format(c.name, c.get_data_hash()) for c in self.get_children()
            ])
        )

    async def get_schema_hash(self):
        return md5(
            ','.join([
                '{}-{}'.format(c.name, c.get_schema_hash()) for c in self.get_children()
            ])
        )


class ParentStore(Store, WithChildren):
    pass
