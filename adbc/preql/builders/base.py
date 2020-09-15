from ..dialect import Dialect, Backend
from .postgres import PostgresBuilder


builders = {
    Backend.POSTGRES: PostgresBuilder(),
    # Backend.MYSQL: MySQLBuilder,
    # Backend.SQLITE: SQLiteBuilder
}


def get_builder(dialect: Dialect):
    global builders
    return builders[dialect.backend]
