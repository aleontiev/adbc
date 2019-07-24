from cleo import Command
from apg.database import Database
from .utils import get_include_exclude
import asyncio
from pprint import pprint


class DiffCommand(Command):
    """
    Diffs two databases

    diff
        {source : Source URL}
        {target : Target URL}
        {--s|namespaces : Namespaces to include or exclude}
        {--t|tables : Tables to include or exclude}
    """

    def handle(self):
        source = self.argument('source')
        target = self.argument('target')
        namespaces = self.option('namespaces') or 'public'
        tables = self.option('tables') or '!awsdms*'

        include_namespaces, exclude_namespaces = get_include_exclude(
            namespaces
        )
        include_tables, exclude_tables = get_include_exclude(
            tables
        )
        source = Database(
            url=source,
            exclude_namespaces=exclude_namespaces,
            exclude_tables=exclude_tables,
            only_tables=include_tables,
            only_namespaces=include_namespaces
        )
        target = Database(
            url=target,
            exclude_namespaces=exclude_namespaces,
            exclude_tables=exclude_tables,
            only_tables=include_tables,
            only_namespaces=include_namespaces
        )
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        result = loop.run_until_complete(source.diff(target))
        pprint(result)
