import asyncio
import uvloop

from cleo import Command
from adbc.database import Database
from adbc.utils import get_include_args
from pprint import pprint


class DiffCommand(Command):
    """
    Diffs two databases (simple version)

    diff
        {source : Source URL}
        {target : Target URL}
        {--s|namespaces=* : Namespaces to include or exclude}
        {--t|tables=* : Tables to include or exclude}
    """

    def handle(self):
        source = self.argument('source')
        target = self.argument('target')
        namespaces = self.option('namespaces') or '*'
        tables = self.option('tables')
        if not tables:
            tables = True
        else:
            tables = get_include_args(tables)
        include = get_include_args(namespaces, truth=tables)

        verbose = self.option('verbose')
        source = Database(
            url=source,
            verbose=verbose,
            include=include
        )
        target = Database(
            url=target,
            verbose=verbose,
            include=include
        )

        uvloop.install()
        pprint(asyncio.run(source.diff(target)))
