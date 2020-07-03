from adbc.logging import Loggable
from adbc.database import Database
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

    def validate_database_url(self, name):
        databases = self.workflow.databases
        if name not in databases:
            raise Exception(f'The provided name "{name}" is not defined in "databases"')

        url = databases[name].get("url")
        if not url:
            raise Exception(f'The database info for "{name}" does not include a URL')

        if not is_dsn(url):
            raise Exception(
                f'The value provided for database "{name}"'
                f' is not a valid URL: "{url}"'
            )

        return url

    def validate_database_config(self, name):
        databases = self.workflow.databases
        if name not in databases:
            raise Exception(f'The provided name "{name}" is not defined in "databases"')
        return databases[name]

    def _validate(
        self, name, read=False, write=False, alter=False, tag=None, prompt=False
    ):
        config = self.config
        datasource = config.get(name)
        if not datasource:
            raise Exception(f'"{name}" is required')

        url = self.validate_database_url(datasource)
        config = self.validate_database_config(datasource)
        database = self.validate_connection(
            name,
            url,
            config,
            read=read,
            write=write,
            alter=alter,
            prompt=prompt,
            tag=tag,
        )
        setattr(self, name, database)

    def validate_connection(
        self,
        name,
        url,
        config,
        read=False,
        write=False,
        alter=False,
        prompt=False,
        tag=None,
    ):
        # TODO: validate read/write/alter permissions
        # for a faster / more proactive error message
        tag = tag or name
        return Database(
            name=name,
            tag=tag,
            prompt=prompt,
            url=url,
            config=config,
            verbose=self.verbose,
        )
