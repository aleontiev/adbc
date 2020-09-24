from .dialect import Backend
from .parsers import get_parser

def parse(
    expression: str,
    backend: Backend
):
    parser = get_parser(backend)
    return parser.parse(expression)
