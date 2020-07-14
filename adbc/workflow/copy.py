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

    async def execute(self):
        start = datetime.now()
        scope = self.scope
        source = self.source
        target = self.target
        results = await source.copy(
            target,
            scope=scope
        )
        end = datetime.now()
        results['duration'] = f"{(end-start).total_seconds():.2f} seconds"
        return results
