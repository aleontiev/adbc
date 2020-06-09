from .step import Step


class InfoStep(Step):
    def validate(self):
        self.only = self.config.get("only", None)
        self._validate("source", read=True)

    async def execute(self):
        return await self.source.get_info(only=self.only)
