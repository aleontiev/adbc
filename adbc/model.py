from .query import Query


class Model(Query):
    def __init__(self, table=None, **kwargs):
        self.table = table
        if 'state' not in kwargs:
            kwargs['state'] = {
                'source': {'table': table.name, 'schema': table.namespace.name}
            }

        super().__init__(**kwargs)

    def __str__(self):
        return f'Model: {self.table}'
