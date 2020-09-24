
from .sql import SQLBuilder


class SqliteBuilder(SQLBuilder):
    IDENTIFIER_QUOTE_CHARACTER = '"'
    LITERAL_QUOTE_CHARACTER = "'"
    AUTOINCREMENT = 'AUTOINCREMENT'
    # when creating tables, SQLite
    # requires that AUTOINCREMENT only be used
    # together with a PRIMARY KEY on the same column definition
    # this means we have to "inline" the PK constraints to the column
    INLINE_PRIMARY_KEYS = True

    def can_defer(self, constraint):
        # only FK constraints can be deferred in SQLite
        return constraint['type'] == 'foreign'
