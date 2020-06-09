from .step import Step


class DiffStep(Step):
    def validate(self):
        self._validate("source", read=True)
        self._validate("target", read=True)
        self.translate = self.config.get("translate", None)

    async def execute(self):
        return await self.source.diff(self.target, translate=self.translate)
