from .step import Step


class InfoStep(Step):
    def validate(self):
        self._validate("source")
        self.data = self.config.get('data', True)
        self.schema = self.config.get('schema', True)
        self.scope = self.config.get('scope', None)
        self.hashes = self.config.get('hashes', False)
        self.refresh = self.config.get('refresh', False)

    async def execute(self):
        return await self.source.get_info(
            data=self.data,
            schema=self.schema,
            scope=self.scope,
            hashes=self.hashes,
            refresh=self.refresh
        )
