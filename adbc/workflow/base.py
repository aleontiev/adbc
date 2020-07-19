from adbc.logging import Loggable

from .debug import DebugStep
from .diff import DiffStep
from .copy import CopyStep
from .info import InfoStep
from .query import QueryStep

from adbc.store import Database


class Workflow(Loggable):
    def __init__(self, name, config, databases, verbose=False):
        self.name = name
        self.config = config
        self.databases = databases
        self.verbose = verbose
        self._databases = {}
        steps = config.get("steps", [])
        if not steps:
            raise ValueError(f'workflow "{name}" has no steps')
        self.steps = [AutoStep(self, step, i+1) for i, step in enumerate(steps)]

    def get_database(self, name, tag=None):
        key = (name, tag)
        if key not in self._databases:
            if name not in self.databases:
                raise Exception(
                    f'cannot find database "{name}" in workflow config'
                )
            config = self.databases[name]
            if isinstance(config, dict):
                prompt = config.get('prompt', False)
                scope = config.get('scope', None)
                url = config.get('url')
            else:
                url = config
                scope = None
                prompt = False

            self._databases[key] = Database(
                name=name,
                tag=tag,
                prompt=prompt,
                url=url,
                scope=scope,
                verbose=self.verbose,
            )
        return self._databases[key]

    async def close(self):
        for database in self._databases.values():
            await database.close()
        self._databases = {}

    async def execute(self):
        results = []
        # execute all steps
        for step in self.steps:
            results.append(await step.execute())

        # close all databases
        await self.close()
        return results


class AutoStep(Loggable):
    def __new__(cls, workflow, config, num):
        if cls is AutoStep:
            type = config.get("type", "").lower()
            if not type:
                raise Exception(f'"type" is required but not provided')
            debug = False
            if type.startswith("?"):
                debug = True
                type = type[1:]
            if type == "copy":
                step = CopyStep(workflow, config, num)
            elif type == "diff":
                step = DiffStep(workflow, config, num)
            elif type == "info":
                step = InfoStep(workflow, config, num)
            elif type == "query" or type == "sql":
                step = QueryStep(workflow, config, num)
            else:
                raise Exception(
                    f'the workflow step type "{type}" is not supported'
                )
            if debug:
                return DebugStep(step)
            else:
                return step
        else:
            return super(AutoStep, cls).__new__(cls)
