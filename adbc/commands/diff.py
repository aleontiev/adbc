import asyncio
import uvloop

from cleo import Command
from adbc.database import Database
from adbc.utils import get_inex_args
from pprint import pprint


class DiffCommand(Command):
    """
    Diffs two databases

    diff
        {source : Source URL}
        {target : Target URL}
        {--s|namespaces=* : Namespaces to include or exclude}
        {--t|tables=* : Tables to include or exclude}
    """

    def handle(self):
        source = self.argument('source')
        target = self.argument('target')
        namespaces = self.option('namespaces') or ['public']
        tables = self.option('tables') or ['~awsdms*']
        verbose = self.option('verbose')

        include_namespaces, exclude_namespaces = get_inex_args(
            namespaces
        )
        include_tables, exclude_tables = get_inex_args(
            tables
        )
        source = Database(
            url=source,
            exclude_namespaces=exclude_namespaces,
            exclude_tables=exclude_tables,
            include_tables=include_tables,
            include_namespaces=include_namespaces,
            verbose=verbose,
        )
        target = Database(
            url=target,
            exclude_namespaces=exclude_namespaces,
            exclude_tables=exclude_tables,
            include_tables=include_tables,
            include_namespaces=include_namespaces,
            verbose=verbose
        )

        uvloop.install()
        pprint(asyncio.run(source.diff(target)))
