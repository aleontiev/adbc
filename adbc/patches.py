from pyaml import UnsafePrettyYAMLDumper, add_representer
from jsondiff import Symbol

# handle Symbols from jsondiff in pyyaml/pyaml
add_representer(
    Symbol,
    lambda s, o: s.represent_scalar('tag:yaml.org,2002:str', str(o))
)
# for pyaml, do not print out aliases
UnsafePrettyYAMLDumper.ignore_aliases = lambda *a: True

ok = 1
