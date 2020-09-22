from .postgres import PostgresBackend
from .sqlite import SqliteBackend

def get_backend(scheme):
    if scheme == 'postgres':
        return PostgresBackend()
    elif scheme == 'sqlite':
        return SqliteBackend()

