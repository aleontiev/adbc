try:
    import uvloop
except (ValueError, ImportError):
    uvloop = None

import asyncio
from pyaml import pprint as yaml_pprint
from pprint import pprint
from cleo import Command
from adbc.config import get_config
from adbc.workflow import WorkflowEngine


class RunCommand(Command):
    """Runs a workflow.

    run
        {workflow : workflow name}
        {--c|config=adbc.yml : config filename}
    """

    def handle(self):
        name = self.argument('workflow')
        config_file = self.option('config')
        config = get_config(config_file)
        verbose = self.option('verbose')
        engine = WorkflowEngine(config, verbose=verbose)
        if uvloop:
            uvloop.install()
        result = asyncio.run(
            engine.run(name)
        )
        result = {'data': result}
        try:
            yaml_pprint(result, safe=False)
        except Exception as e:
            print(f'{e.__class__} while formatting: {e}')
            pprint(result)
