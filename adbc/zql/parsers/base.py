from ..dialect import Backend
from .postgres import PostgresParser
from .sqlite import SqliteParser


parsers = {
    Backend.POSTGRES: PostgresParser(),
    Backend.SQLITE: SqliteParser()
}


def get_parser(backend: Backend):
    global parsers
    return parsers[backend]
