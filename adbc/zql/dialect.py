from dataclasses import dataclass
from enum import Enum

class Backend(Enum):
    POSTGRES = 1
    MYSQL = 2
    SQLITE = 3


class ParameterStyle(Enum):
    NUMERIC = 1            # SELECT * FROM user WHERE id = :1
                                # DB-API2
    DOLLAR_NUMERIC = 2     # SELECT * FROM user WHERE id = $1
                                # e.g. asyncpg
    QUESTION_MARK = 3      # SELECT * FROM user WHERE id = ?
                                # DB-API2, e.g. aiosqlite
    FORMAT = 4             # SELECT * FROM user WHERE id = %s
                                # DB-API2, e.g. aiomysql, psycopg2
    NAMED = 5              # SELECT * FROM user WHERE id = :id
                                # DB-API2, e.g. aiosqlite
    DOLLAR_NAMED = 6       # SELECT * FROM user WHERE id = $id
                                # ???


@dataclass
class Dialect:
    backend: Backend
    style: ParameterStyle


def get_default_style() -> ParameterStyle:
    return ParameterStyle.FORMAT
