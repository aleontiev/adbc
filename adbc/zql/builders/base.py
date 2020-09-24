from ..dialect import Dialect, Backend
from .postgres import PostgresBuilder
from .sqlite import SqliteBuilder


builders = {
    Backend.POSTGRES: PostgresBuilder(),
    Backend.SQLITE: SqliteBuilder()
    # Backend.MYSQL: MySQLBuilder,
}


def get_builder(dialect: Dialect):
    global builders
    return builders[dialect.backend]
