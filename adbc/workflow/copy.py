import uuid
from datetime import datetime

from .step import Step


class CopyStep(Step):
    parallel_copy = True
    check_all = True

    def validate(self):
        # unique prefix for this job
        self.prefix = str(uuid.uuid4()) + "/"
        self._validate("source", read=True)
        self._validate("target", read=True, write=True, alter=True)
        self.translate = self.config.get("translate", None)

    async def execute(self):
        start = datetime.now()
        translate = self.translate
        source = self.source
        target = self.target
        results = await source.copy(
            target,
            translate=translate,
        )

        end = datetime.now()
        results['duration'] = f"{(end-start).total_seconds():.2f} seconds"
        return results
