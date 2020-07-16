from .step import Step


class QueryStep(Step):
    def validate(self):
        self._validate("source")
        self.query = self.config.get('query', None)
        self.fetch = self.config.get('fetch', False)

    async def execute(self):
        fetch = self.fetch
        if fetch == 'one':
            method = 'query_one_row'
        elif fetch:
            method = 'query'
        else:
            method = 'execute'
        return await getattr(self.source, method)(
            self.query
        )
