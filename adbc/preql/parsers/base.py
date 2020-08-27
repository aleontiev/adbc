from ..dialect import Backend
from .postgres import PostgresParser


parsers = {
    Backend.POSTGRES: PostgresParser(),
}


def get_parser(backend: Backend):
    global parsers
    return parsers[backend]
