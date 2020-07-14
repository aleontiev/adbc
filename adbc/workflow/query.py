from .step import Step


class QueryStep(Step):
    def validate(self):
        self._validate("source")
        self.query = self.config.get('query', None)

    async def execute(self):
        return await self.source.execute(
            self.query
        )
