from adbc.store import Loggable

from .debug import DebugStep
from .diff import DiffStep
from .copy import CopyStep
from .info import InfoStep
from .flow import FlowStep


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
            command = config.get("command", "").lower()
            if not command:
                raise Exception(f'"command" is required but not provided')
            debug = False
            if command.startswith("?"):
                debug = True
                command = command[1:]
            if command == "copy":
                step = CopyStep(workflow, config)
            elif command == "diff":
                step = DiffStep(workflow, config)
            elif command == "info":
                step = InfoStep(workflow, config)
            elif command == 'flow':
                step = FlowStep(workflow, config)
            else:
                raise Exception(f'the provided command "{command}" is not supported')
            if debug:
                return DebugStep(step)
            else:
                return step
        else:
            return super(AutoStep, cls).__new__(cls)
