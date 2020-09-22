
from .sql import SQLBuilder


class SqliteBuilder(SQLBuilder):
    IDENTIFIER_QUOTE_CHARACTER = '"'
    LITERAL_QUOTE_CHARACTER = "'"
