from .query import Query
from .executors import get_executor


class Model(Query):
    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.executor = get_executor(self.table)

    def __str__(self):
        return f'Model: {self.table}'
