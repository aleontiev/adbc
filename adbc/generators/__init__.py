from typing import List, Dict
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
    null: bool = True


@dataclass_json
@dataclass
class Constraint(Generator):
    type: str
    columns: List[str] = field(default_factory=list)
    deferrable: bool = False
    deferred: bool = False
    check: str = None
    related_columns: List[str] = field(default_factory=list)
    related_name: str = None

    def __post_init__(self):
        if len(self.type) > 0:
            self.type = self.type[0]


@dataclass_json
@dataclass
class Index(Generator):
    type: str
    columns: List[str] = field(default_factory=list)
    unique: bool = False
    primary: bool = False


@dataclass_json
@dataclass
class TableSchema(Generator):
    columns: Dict[str, Column] = field(default_factory=dict)
    constraints: Dict[str, Constraint] = field(default_factory=dict)
    indexes: Dict[str, Index] = field(default_factory=dict)


@dataclass_json
@dataclass
class Table(Generator):
    schema: Dict[str, TableSchema] = field(default_factory=dict)


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
