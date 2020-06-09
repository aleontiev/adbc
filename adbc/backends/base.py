class DatabaseBackend(object):
    @classmethod
    def has(cls, feature):
        return getattr(cls, f'has_{feature}', False)

    @classmethod
    def get_query(cls, name, *args, **kwargs):
        method = f'get_{name}_query'
        return getattr(cls, method)(*args, **kwargs)
