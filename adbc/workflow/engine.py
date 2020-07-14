from .base import Workflow

class WorkflowEngine(object):
    def __init__(self, config: dict, verbose: bool = False):
        self.config = config
        self.verbose = verbose

    async def run(self, name):
        databases = self.config.get('databases')
        if not databases:
            raise ValueError(f'no databases in config')

        workflow = self.config.get('workflows', {}).get(name)
        if not workflow:
            raise ValueError(f'no workflow {name} in config')

        workflow = Workflow(
            name,
            workflow,
            databases,
            verbose=self.verbose
        )
        return await workflow.execute()
