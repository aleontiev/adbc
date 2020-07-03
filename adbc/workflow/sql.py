from .step import Step


class SQLStep(Step):
    def validate(self):
        prompt = self.config.get('prompt', False)
        self._validate("source", read=True, write=True, prompt=prompt)
        self.query = self.config.get('query', None)

    async def execute(self):
        return await self.source.execute(
            self.query
        )
