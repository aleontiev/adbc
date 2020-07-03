from adbc.store import Loggable

from .debug import DebugStep
from .diff import DiffStep
from .copy import CopyStep
from .info import InfoStep
from .sql import SQLStep


class Workflow(Loggable):
    def __init__(self, name, config, databases, verbose=False):
        self.name = name
        self.config = config
        self.databases = databases
        self.verbose = verbose
        steps = config.get("steps", [])
        if not steps:
            raise ValueError(f'workflow "{name}" has no steps')
        self.steps = [AutoStep(self, step) for step in steps]

    async def execute(self):
        results = []
        for step in self.steps:
            results.append(await step.execute())
        return results


class AutoStep(Loggable):
    def __new__(cls, workflow, config):
        if cls is AutoStep:
            type = config.get("type", "").lower()
            if not type:
                raise Exception(f'"type" is required but not provided')
            debug = False
            if type.startswith("?"):
                debug = True
                type = type[1:]
            if type == "copy":
                step = CopyStep(workflow, config)
            elif type == "diff":
                step = DiffStep(workflow, config)
            elif type == "info":
                step = InfoStep(workflow, config)
            elif type == "sql":
                step = SQLStep(workflow, config)
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
