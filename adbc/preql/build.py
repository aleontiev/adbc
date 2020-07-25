"""preql compiler"""
from typing import List, Dict
from .dialect import Dialect
from .builders import get_builder

def build(
    query: dict,
    dialect: Dialect,
) -> List[dict]:
    builder = get_builder(dialect)
    return builder.build(query, style=dialect.style)
