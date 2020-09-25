from .dialect import Backend
from .parsers import get_parser

def parse_expression(
    expression: str,
    backend: Backend
):
    """parse SQL expression into ZQL"""
    parser = get_parser(backend)
    return parser.parse_expression(expression)


def parse_statement(
    statement: str,
    backend: Backend
):
    """parse SQL into ZQL"""
    parser = get_parser(backend)
    return parser.parse_statement(statement)
