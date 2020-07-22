"""preql compiler"""
from typing import List, Dict
from .dialect import Dialect
from .backends import get_backend

def build(
    query: dict,
    dialect: Dialect,
) -> List[dict]:
    # 1. validate query against the PreQL JSONSchema
    # TODO

    # 2. get a backend implementation of PreQL
    backend = get_backend(dialect)
    return backend.build(query, style=dialect.style)
