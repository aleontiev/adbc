from adbc.logging import Loggable
from adbc.store import Database
from adbc.utils import is_dsn


class Step(Loggable):
    _log_name = "adbc.workflow"

    def __init__(self, workflow, config, **kwargs):
        super().__init__(**kwargs)
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
        database_name = self.config.get(name)
        database = self.workflow.get_database(
            database_name,
            tag=tag
        )
        setattr(self, name, database)
        return database
