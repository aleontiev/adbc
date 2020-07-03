from .step import Step


class InfoStep(Step):
    def validate(self):
        prompt = self.config.get('prompt', False)
        self._validate("source", read=True, prompt=prompt)
        self.data = self.config.get('data', True)
        self.schema = self.config.get('schema', True)
        self.scope = self.config.get('scope', None)

    async def execute(self):
        return await self.source.get_info(
            data=self.data,
            schema=self.schema,
            scope=self.scope
        )
