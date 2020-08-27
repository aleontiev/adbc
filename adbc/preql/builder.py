"""preql compiler"""
from typing import List, Dict, Union
from .dialect import Dialect
from .builders import get_builder

def build(
    query: dict,
    dialect: Dialect,
    combine: bool = False
) -> Union[List[tuple], tuple]:
    builder = get_builder(dialect)
    style = dialect.style
    result = builder.build(query, style=style)
    if not combine:
        return result

    # combine results into a single query
    results = []
    params = builder.get_empty_parameters(style)
    for r in result:
        results.append(r[0])
        builder.extend_parameters(params, r[1])
    return (
        builder.combine(results, separator=';\n'),
        params
    )
