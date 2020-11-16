from adbc.logging import Loggable
from adbc.store import Database


class Step(Loggable):
    _log_name = "adbc.workflow"

    def __init__(self, workflow, config, num, **kwargs):
        self.name = f'{workflow.name}#{num}'
        super().__init__(**kwargs)
        self.num = num
        self.workflow = workflow
        self.verbose = self.workflow.verbose
        self.config = config
        self.validate()

    def validate(self):
        raise NotImplementedError()

    async def execute(self):
        raise NotImplementedError()

    def _validate(
        self, name, tag=None
    ):
        tag = tag or name
        if name not in self.config:
            type = self.config['type']
            raise Exception(
                f'could not find database "{name}" '
                f'in workflow "{self.workflow.name}" '
                f'on step {self.num} ({type})'
            )
        database_name = self.config.get(name)
        database = self.workflow.get_database(
            database_name,
            tag=tag
        )
        setattr(self, name, database)
        return database
