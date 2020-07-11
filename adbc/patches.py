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
# for pyaml, do not print out aliases
UnsafePrettyYAMLDumper.ignore_aliases = lambda *a: True

ok = 1
