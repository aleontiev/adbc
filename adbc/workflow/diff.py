from .step import Step


class DiffStep(Step):
    def validate(self):
        self.scope = self.config.get("scope", None)
        self.refresh = self.config.get('refresh', False)
        self._validate("source")
        self._validate("target")

    async def execute(self):
        return await self.source.diff(
            self.target,
            scope=self.scope,
            refresh=self.refresh
        )
