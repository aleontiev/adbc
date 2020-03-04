from adbc.utils import is_dsn
from adbc.database import Database


class Workflow(object):
    def __init__(self, name, config, databases):
        self.name = name
        self.config = config
        self.databases = databases
        steps = config.get('steps', [])
        if not steps:
            raise ValueError(f'workflow "{name}" has no steps')
        self.steps = [WorkflowStep(self, step) for step in steps]

    async def execute(self):
        results = []
        for step in self.steps:
            results.append(await step.execute())
        return results


class WorkflowStep(object):
    def __new__(cls, workflow, config):
        if cls is WorkflowStep:
            command = config.get("command", "").lower()
            if not command:
                raise Exception(f'"command" is required but not provided')
            if command == "copy":
                return CopyStep(workflow, config)
            elif command == "diff":
                return DiffStep(workflow, config)
            elif command == "info":
                return InfoStep(workflow, config)
            else:
                raise Exception(f'the provided command "{command}" is not supported')
        else:
            return super(WorkflowStep, cls).__new__(cls)

    def __init__(self, workflow, config):
        self.workflow = workflow
        self.config = config
        self.validate()

    def validate(self):
        raise NotImplementedError()

    async def execute(self):
        raise NotImplementedError()

    def validate_database_url(self, name):
        databases = self.workflow.databases
        if name not in databases:
            raise Exception(
                f'The provided name "{name}" is not defined in "databases"'
            )

        url = databases[name].get("url")
        if not url:
            raise Exception(f'the database info for "{name}" does not include a URL')

        if not is_dsn(url):
            raise Exception(
                f'The url provided for database "{name}"'
                f' is not a valid connection string: "{url}"'
            )

        return url

    def validate_database_schemas(self, name):
        databases = self.workflow.databases
        if name not in databases:
            raise Exception(
                f'The provided name "{name}" is not defined in "databases"'
            )

        schemas = databases[name].get("schemas")
        # if schemas is None/undefined, assume include all
        return schemas if schemas is not None else True

    def validate_credentials(self, url, read=False, write=False):
        pass


class CopyStep(WorkflowStep):
    def validate(self):
        config = self.config
        source = config.get("source")
        target = config.get("target")
        if not source or not target:
            raise Exception('"source" and "target" are required')

        self.source_url = self.validate_database_url(source)
        self.target_url = self.validate_database_url(target)
        self.validate_credentials(self.source_url, read=True)
        self.validate_credentials(self.target_url, read=True, write=True)

    async def execute(self):
        # copy
        # S schemas
        # ... T tables per schema
        # ....... R*C data points per table
        pass


class InfoStep(WorkflowStep):
    def validate(self):
        config = self.config
        self.source = source = config.get("source")
        if not source:
            raise Exception('"source" is required')

        self.source_url = self.validate_database_url(source)
        self.source_schemas = self.validate_database_schemas(source)
        self.validate_credentials(self.source_url, read=True)

    async def execute(self):
        database = Database(
            name=self.source,
            url=self.source_url,
            include=self.source_schemas
        )
        return await database.get_diff_data()


class DiffStep(WorkflowStep):
    pass
