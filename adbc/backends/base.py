class DatabaseBackend(object):
    FUNCTIONS = {}

    @classmethod
    def has(cls, feature):
        return getattr(cls, f'has_{feature}', False)

    @classmethod
    def get_query(cls, name, *args, **kwargs):
        method = f'get_{name}_query'
        return getattr(cls, method)(*args, **kwargs)

    def has_function(cls, fn):
        return fn in cls.FUNCTIONS

    @staticmethod
    def get_include_zql(include, table, column, tag=None):
        clauses = []
        column = f'{table}.{column}'
        if not include or include is True:
            return None

        includes = excludes = False
        for key, should in include.items():
            should_dict = isinstance(should, dict)
            if should_dict:
                should_dict = should
                if 'enabled' in should:
                    should = should['enabled']
            if not should:
                continue
            should = bool(should)
            if key.startswith('~'):
                should = not should
                key = key[1:]

            wild = False
            if "*" in key:
                wild = True
                operator = "~~" if should else "!~~"
                key = key.replace("*", "%")
            else:
                operator = "=" if should else "!="

            if tag is None:
                name = key
            else:
                name = should_dict.get(tag, key) if should_dict else key
                if wild and should_dict and tag in should_dict:
                    raise ValueError(f"Cannot have tag '{name}' for wild key '{key}'")

            clauses.append({operator: [column, f"'{name}'"]})
            if should:
                includes = True
            else:
                excludes = True

        if len(clauses) > 1:
            if includes and not excludes:
                union = "or"
            else:
                union = "and"
            return {union: clauses}
        elif len(clauses) == 1:
            return clauses[0]
        else:
            return None

    @staticmethod
    async def initialize(database):
        pass
