from cleo import Command
from adbc.config import read_config_file, hydrate_config, get_initial_context
from adbc.workflow import Workflow


class RunCommand(Command):
    """Runs a workflow.

    run
        {command : command name}
        {--c|config=adbc.yml : config filename}
    """

    def handle(self):
        command_name = self.argument('command')
        config_file = self.option('config')
        config = hydrate_config(
            read_config_file(config_file),
            context=get_initial_context()
        )
        workflows = config.get('workflows', {})
        databases = config.get('databases', {})
        workflow_data = workflows.get(command_name, None)
        if not workflow_data:
            raise Exception(f'No workflow data for "{command_name}"')

        workflow = Workflow(workflow_data, databases)
        workflow.execute()
