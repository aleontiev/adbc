import uuid
from pyaml import UnsafePrettyYAMLDumper, add_representer
import jsondiff
from .symbols import insert, delete

# replace object symbols with more portable JSON ones: + and -
jsondiff.insert = insert
jsondiff.delete = delete

# handle Symbols from jsondiff in pyyaml/pyaml
add_representer(
    jsondiff.Symbol,
    lambda s, o: s.represent_scalar('tag:yaml.org,2002:str', str(o))
)
add_representer(
    uuid.UUID,
    UnsafePrettyYAMLDumper.represent_stringish
)
try:
    import asyncpg
except ImportError:
    pass
else:
    add_representer(
        asyncpg.Record,
        UnsafePrettyYAMLDumper.represent_dict
    )

# for pyaml, do not print out aliases
UnsafePrettyYAMLDumper.ignore_aliases = lambda *a: True

ok = 1
