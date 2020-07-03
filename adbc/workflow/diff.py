from .step import Step


class DiffStep(Step):
    def validate(self):
        prompt = self.config.get('prompt', False)
        self._validate("source", read=True, prompt=prompt)
        self._validate("target", read=True, prompt=prompt)
        self.scope = self.config.get("scope", None)

    async def execute(self):
        return await self.source.diff(self.target, scope=self.scope)
