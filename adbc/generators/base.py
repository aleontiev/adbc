from typing import List, Dict, Union
from dataclasses import dataclass, field
from dataclasses_json import dataclass_json


class Generator(object):
    @property
    def data(self):
        return self.to_dict()

@dataclass_json
@dataclass
class Column(Generator):
    type: str
    default: str = None
    null: bool = False
    sequence: Union[bool, str] = False
    primary: Union[bool, str] = False
    unique: Union[bool, str] = False
    related: dict = None


@dataclass_json
@dataclass
class Constraint(Generator):
    type: str
    columns: List[str] = field(default_factory=list)
    deferrable: bool = False
    deferred: bool = False
    check: str = None
    related_columns: List[str] = None
    related_name: str = None


@dataclass_json
@dataclass
class Index(Generator):
    type: str
    columns: List[str] = field(default_factory=list)
    unique: bool = False
    primary: bool = False


@dataclass_json
@dataclass
class Table(Generator):
    type: str = 'table'
    columns: Dict[str, Column] = field(default_factory=dict)
    constraints: Dict[str, Constraint] = field(default_factory=dict)
    indexes: Dict[str, Index] = field(default_factory=dict)


registry = {
    'table': Table,
    'index': Index,
    'constraint': Constraint,
    'column': Column
}


def G(name: str, **kwargs) -> dict:
    if name not in registry:
        raise ValueError(f'there is no generator for "{name}"')

    return registry[name](**kwargs).to_dict()
