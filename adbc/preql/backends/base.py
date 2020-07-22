from .dialect import Dialect, Backend
from .postgres import PostgresBackend


backends = {
    Backend.POSTGRES: PostgresBackend(),
    # Backend.MYSQL: MySQLBackend,
    # Backend.SQLITE: SQLiteBackend
}
def get_backend(dialect: Dialect):
    return backends[dialect.backend]
