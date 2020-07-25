from .dialect import Dialect, Builder
from .postgres import PostgresBuilder


builders = {
    Builder.POSTGRES: PostgresBuilder(),
    # Builder.MYSQL: MySQLBuilder,
    # Builder.SQLITE: SQLiteBuilder
}


def get_builder(dialect: Dialect):
    global builders
    return builders[dialect.backend]
