import uuid
from datetime import datetime

from .step import Step


class CopyStep(Step):
    parallel_copy = True
    check_all = True

    def validate(self):
        # unique prefix for this job
        self.prefix = str(uuid.uuid4()) + "/"
        self._validate("source")
        self._validate("target")
        self.scope = self.config.get("scope", None)
        self.refresh = self.config.get("refresh", False)
        self.final_diff = self.config.get('final_diff', True)

    async def execute(self):
        start = datetime.now()
        scope = self.scope
        source = self.source
        target = self.target
        results = await source.copy(
            target,
            scope=scope,
            final_diff=self.final_diff
        )
        end = datetime.now()
        results['duration'] = f"{(end-start).total_seconds():.2f} seconds"
        return results
