from adbc.preql.dialect import ParameterStyle, get_default_style
import re
from typing import List, Union


class SQLBackend:
    IDENTIFIER_QUOTE_CHARACTER = '"'
    COMMANDS = {
        # data
        'select': True,
        'update': True,
        'insert': True,
        'delete': True,
        'truncate': True,
        # metadata
        'show': True,
        'create_database': True,
        'create_schema': True,
        'create_table': True,
        'create_column': True,
        'create_index': True,
        'create_sequence': True,
        'create_view': True,
        'drop_view': True,
        'drop_table': True,
        'drop_column': True,
        'drop_index': True,
        'drop_sequence': True,
        'alter_table': True,
    }

    def build(self, query: dict, style: ParameterStyle = None) -> List[tuple]:
        if style is None:
            style = get_default_style()

        keys = list(query.keys())
        if len(keys) != 1:
            raise ValueError('query root must have one key (command name)')

        command_name = keys[0]
        command = query[command_name]
        build_method = f'build_{command_name}'
        return getattr(self, build_method)(command)

    def escape_identifier(self, identifier: Union[list, str]):
        if isinstance(identifier, list):
            identifier = [identifier]
            return [self.escape_identifier(ident) for ident in identifier]
        if self.IDENTIFIER_QUOTE_CHARACTER in identifier:
            # TODO escape the quote character with \
            return identifier

    def format_identifier(self, identifier: Union[list, str]):
        identifier = self.escape_identifier(identifier)
        quote = self.IDENTIFIER_QUOTE_CHARACTER
        if not isinstance(identifier, list):
            if '.' in identifier:
                identifier = identifier.split('.')
            else:
                identifier = [identifier]

        return ''.join([f'{quote}{ident}{quote}' for ident in identifier])

    def build_create_database(self, command: str) -> List[tuple]:
        database = self.format_identifier(command)
        query = 'CREATE DATABASE {database}'
        return [(query, None)]

    def build_create_schema(self, command: str) -> List[tuple]:
        schema = self.format_identifier(command)
        query = 'CREATE SCHEMA {schema}'
        return [(query, None)]

    def build_create_table(self, command: dict) -> List[tuple]:
        name = command['name']
        columns = command.get('columns', None)
        constraints = command.get('constraints', None)
        as_ = command.get('as', None)

    def build_select(self, command: dict) -> List[tuple]:
        return [(query, parameters)]
