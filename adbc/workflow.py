from adbc.utils import is_dsn


class Workflow(object):
    def __new__(cls, config, databases):
        if cls is Workflow:
            method = config.get("method", "").lower()
            if not method:
                raise Exception(f'"method" is required but not provided')
            if method == "copy":
                return CopyWorkflow(config, databases)
            elif method == "diff":
                return DiffWorkflow(config, databases)
            elif method == "info":
                return InfoWorkflow(config, databases)
            else:
                raise Exception(f'the provided method "{method}" is not supported')
        else:
            return super(Workflow, cls).__new__(cls)

    def __init__(self, config, databases):
        self.config = config
        self.databases = databases
        self.validate()

    def validate(self):
        raise NotImplementedError()

    def execute(self):
        raise NotImplementedError()

    def validate_database_url(self, name):
        if name not in self.databases:
            raise Exception(
                f'The provided name "{name}" is not defined in "databases"'
            )

        url = self.databases[name].get("url")
        if not url:
            raise Exception(f'the database info for "{name}" does not include a URL')

        if not is_dsn(url):
            raise Exception(
                f'The url provided for database "{name}"'
                f' is not a valid connection string: "{url}"'
            )

        return url

    def validate_credentials(self, url, read=False, write=False):
        pass


class CopyWorkflow(Workflow):
    def validate(self):
        config = self.config
        source = config.get("source")
        target = config.get("target")
        if not source or not target:
            raise Exception('"source" and "target" are required')

        source_url = self.validate_database_url(source)
        target_url = self.validate_database_url(target)
        self.validate_credentials(source_url, read=True)
        self.validate_credentials(target_url, read=True, write=True)

    def execute(self):
        # copy
        # S schemas
        # ... T tables per schema
        # ....... R*C data points per table
        pass


class InfoWorkflow(Workflow):
    def validate(self):
        config = self.config
        source = config.get("source")
        if not source:
            raise Exception('"source" is required')

        source_url = self.validate_database_url(source)
        self.validate_credentials(source_url, read=True)


class DiffWorkflow(Workflow):
    pass
