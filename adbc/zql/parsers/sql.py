import re

from adbc.store.table import Table
from adbc.utils import named_dict_to_list
from pyparsing import (
    CaselessKeyword, Forward, Word, Regex, alphanums,
    delimitedList, Suppress, Optional, Group, OneOrMore
)



class SQLParser():
    """General SQL Parser

    Based on and inspired by ddlparser
    Modified to:
    - support SQLite edge cases:
        - AUTO_INCREMENT vs AUTOINCREMENT
    - support other statement types: (WIP)
        - CREATE INDEX
        - CREATE SCHEMA
        - CREATE SEQUENCE
        - CREATE DATABASE
    - replaces parse results with ZQL
    - adds parse_expression to support parsing expressions into ZQL (WIP)
    """

    FUNCTION_REGEX = re.compile(r'^([a-zA-Z][0-9a-zA-Z._]*)\(([^)]*)\)$')

    INITIALLY_DEFERRED, INITIALLY_IMMEDIATE, DEFERRABLE, NOT_DEFERRABLE = map(CaselessKeyword, "INITIALLY DEFERRED, INITIALLY IMMEDIATE, DEFERRABLE, NOT DEFERRABLE".replace(", ", ",").split(","))
    LPAR, RPAR, COMMA, SEMICOLON, DOT, DOUBLEQUOTE, BACKQUOTE, SPACE = map(Suppress, "(),;.\"` ")
    CREATE, TABLE, TEMP, CONSTRAINT, NOT_NULL, PRIMARY_KEY, UNIQUE, UNIQUE_KEY, FOREIGN_KEY, REFERENCES, KEY, CHAR_SEMANTICS, BYTE_SEMANTICS = \
        map(CaselessKeyword, "CREATE, TABLE, TEMP, CONSTRAINT, NOT NULL, PRIMARY KEY, UNIQUE, UNIQUE KEY, FOREIGN KEY, REFERENCES, KEY, CHAR, BYTE".replace(", ", ",").split(","))
    TYPE_UNSIGNED, TYPE_ZEROFILL = \
        map(CaselessKeyword, "UNSIGNED, ZEROFILL".replace(", ", ",").split(","))
    COL_ATTR_DISTKEY, COL_ATTR_SORTKEY, COL_ATTR_CHARACTER_SET = \
        map(CaselessKeyword, "DISTKEY, SORTKEY, CHARACTER SET".replace(", ", ",").split(","))
    FK_MATCH = \
        CaselessKeyword("MATCH") + Word(alphanums + "_")
    FK_ON, FK_ON_OPT_RESTRICT, FK_ON_OPT_CASCADE, FK_ON_OPT_SET_NULL, FK_ON_OPT_NO_ACTION = \
        map(CaselessKeyword, "ON, RESTRICT, CASCADE, SET NULL, NO ACTION".replace(", ", ",").split(","))
    FK_ON_DELETE = \
        FK_ON + CaselessKeyword("DELETE") + (FK_ON_OPT_RESTRICT | FK_ON_OPT_CASCADE | FK_ON_OPT_SET_NULL | FK_ON_OPT_NO_ACTION)
    FK_ON_UPDATE = \
        FK_ON + CaselessKeyword("UPDATE") + (FK_ON_OPT_RESTRICT | FK_ON_OPT_CASCADE | FK_ON_OPT_SET_NULL | FK_ON_OPT_NO_ACTION)
    SUPPRESS_QUOTE = BACKQUOTE | DOUBLEQUOTE

    COMMENT = Suppress("--" + Regex(r".+"))

    CREATE_TABLE_STATEMENT = Suppress(CREATE) + Optional(TEMP)("temporary") + Suppress(TABLE) + Optional(Optional(CaselessKeyword("IF NOT EXISTS")("maybe"))) \
        + Optional(SUPPRESS_QUOTE) + Optional(Word(alphanums + "_")("schema") + Optional(SUPPRESS_QUOTE) + DOT + Optional(SUPPRESS_QUOTE)) + Word(alphanums + "_<>")("table") + Optional(SUPPRESS_QUOTE) \
        + LPAR \
        + delimitedList(
            OneOrMore(
                COMMENT
                |
                # Ignore Index
                Suppress(KEY + Word(alphanums + "_'`() "))
                |
                Group(
                    Optional(Suppress(CONSTRAINT) + Optional(SUPPRESS_QUOTE) + Word(alphanums + "_")("name") + Optional(SUPPRESS_QUOTE))
                    + (
                        (
                            (PRIMARY_KEY ^ UNIQUE ^ UNIQUE_KEY ^ NOT_NULL)("type")
                            + Optional(SUPPRESS_QUOTE) + Optional(Word(alphanums + "_"))("name") + Optional(SUPPRESS_QUOTE)
                            + LPAR + Group(delimitedList(Optional(SUPPRESS_QUOTE) + Word(alphanums + "_") + Optional(SUPPRESS_QUOTE)))("constraint_columns") + RPAR
                        )
                        |
                        (
                            (FOREIGN_KEY)("type")
                            + LPAR + Group(delimitedList(Optional(SUPPRESS_QUOTE) + Word(alphanums + "_") + Optional(SUPPRESS_QUOTE)))("constraint_columns") + RPAR
                            + Optional(Suppress(REFERENCES)
                                + Optional(SUPPRESS_QUOTE) + Word(alphanums + "_")("references_table") + Optional(SUPPRESS_QUOTE)
                                + LPAR + Group(delimitedList(Optional(SUPPRESS_QUOTE) + Word(alphanums + "_") + Optional(SUPPRESS_QUOTE)))("references_columns") + RPAR
                                # + Optional(FK_MATCH)("references_fk_match")  # MySQL
                                # + Optional(FK_ON_DELETE)("references_fk_on_delete")  # MySQL
                                # + Optional(FK_ON_UPDATE)("references_fk_on_update")  # MySQL
                            )
                        )
                        & Optional(NOT_DEFERRABLE ^ DEFERRABLE)("deferrable")  # Postgres, Oracle
                        & Optional(INITIALLY_DEFERRED ^ INITIALLY_IMMEDIATE)("deferred")  # Postgres, Oracle
                    )
                )("constraint")
                |
                Group(
                    ((SUPPRESS_QUOTE + Word(alphanums + " _")("name") + SUPPRESS_QUOTE) ^ (Optional(SUPPRESS_QUOTE) + Word(alphanums + "_")("name") + Optional(SUPPRESS_QUOTE)))
                    + Optional(Group(
                        Group(
                            Word(alphanums + "_")
                            + Optional(CaselessKeyword("WITHOUT TIME ZONE") ^ CaselessKeyword("WITH TIME ZONE") ^ CaselessKeyword("PRECISION") ^ CaselessKeyword("VARYING"))
                        )("type_name")
                        + Optional(LPAR + Regex(r"[\d\*]+\s*,*\s*\d*")("length") + Optional(CHAR_SEMANTICS | BYTE_SEMANTICS)("semantics") + RPAR)
                        + Optional(TYPE_UNSIGNED)("unsigned")
                        + Optional(TYPE_ZEROFILL)("zerofill")
                    )("type"))
                    + Optional(Word(r"\[\]"))("array_brackets")
                    + Optional(
                        Regex(r"(?!--)", re.IGNORECASE)
                        + Group(
                            Optional(Regex(r"\b(?:NOT\s+)NULL?\b", re.IGNORECASE))("null")
                            & Optional(Regex(r"\bAUTO(?:_)?INCREMENT\b", re.IGNORECASE))("auto_increment")
                            & (
                                Optional(Regex(r"\b(UNIQUE|PRIMARY)(?:\s+KEY)?\b", re.IGNORECASE))("key")
                                | (
                                    (FOREIGN_KEY)("type")
                                    + LPAR + Group(delimitedList(Optional(SUPPRESS_QUOTE) + Word(alphanums + "_") + Optional(SUPPRESS_QUOTE)))("constraint_columns") + RPAR
                                    + Optional(
                                        Suppress(REFERENCES) +
                                        Optional(SUPPRESS_QUOTE) +
                                        Word(alphanums + "_")("references_table") +                 
                                        Optional(SUPPRESS_QUOTE) +
                                        LPAR +
                                        Group(
                                            delimitedList(
                                                Optional(SUPPRESS_QUOTE) + 
                                                Word(alphanums + "_") + 
                                                Optional(SUPPRESS_QUOTE)
                                            )
                                        )("references_columns") +
                                        RPAR
                                    )
                                )
                            )
                            & Optional(Regex(
                                r"\bDEFAULT\b\s+(?:((?:[A-Za-z0-9_\.\'\" -\{\}]|[^\x01-\x7E])*\:\:(?:character varying)?[A-Za-z0-9\[\]]+)|(?:\')((?:\\\'|[^\']|,)+)(?:\')|(?:\")((?:\\\"|[^\"]|,)+)(?:\")|([^,\s]+))",
                                re.IGNORECASE))("default")
                            & Optional(Regex(r"\bCOMMENT\b\s+(\'(\\\'|[^\']|,)+\'|\"(\\\"|[^\"]|,)+\"|[^,\s]+)", re.IGNORECASE))("comment")
                            & Optional(Regex(r"\bENCODE\s+[A-Za-z0-9]+\b", re.IGNORECASE))("encode")  # Redshift
                            & Optional(COL_ATTR_DISTKEY)("distkey")  # Redshift
                            & Optional(COL_ATTR_SORTKEY)("sortkey")  # Redshift
                            & Optional(Suppress(COL_ATTR_CHARACTER_SET) + Word(alphanums + "_")("character_set"))  # MySQL
                        )("constraint")
                    )
                )("column")
                |
                COMMENT
            )
        )("items")

    PARSE = Forward()
    PARSE << OneOrMore(COMMENT | CREATE_TABLE_STATEMENT)


    def __init__(self, sql=None):
        self.sql = sql

    def remove_cast(self, literal: str):
        if '::' in literal:
            literal = literal.split('::')[0]
        return literal

    def parse_literal(self, literal: str):
        return self.remove_cast(literal)

    def parse_expression(self, expression: str):
        # TODO: replace with real SQL parsing
        # this is super hacky
        if expression is None or not isinstance(expression, str):
            return expression

        if '(' in expression:
            match = self.FUNCTION_REGEX.match(expression)
            if match:
                fn = match.group(1)
                arguments = match.group(2)

            # assume 1 variable function call only
            # TODO: support multi-variable calls
            arguments = self.parse_literal(arguments)
            return {fn: arguments}
        else:
            result = self.parse_literal(expression)
        return result

    def get_column_type(self, type, brackets=None):
        if not type:
            # typeless, e.g. SQLite
            return None
        if 'type_name' not in type:
            raise ValueError(f'{type}: missing type name')

        type_name = ' '.join(type['type_name']).lower()
        length = type.get('length')
        semantics = type.get('semantics')
        unsigned = type.get('unsigned')
        zerofill = type.get('zerofill')
        optionals = []
        if length:
            if semantics:
                optionals.append(f'({length} {semantics})')
            else:
                optionals.append(f'({length})')
        if unsigned:
            optionals.append(unsigned)
        if zerofill:
            optionals.append(zerofill)
        if optionals:
            type_name += ' '.join(optionals)
        if brackets:
            type_name += brackets
        # e.g. character varying (10)
        return type_name

    def get_column_definition(self, column):
        result = {}

        result['name'] = column['name']
        result['type'] = self.get_column_type(column.get('type'), column.get('array_brackets'))
        result['default'] = column.get('default')
        constraint = column.get('constraint', {})
        result['null'] = 'NOT NULL' not in constraint.get('null', '').upper()
        key = constraint.get('key', '')
        result['primary'] = key.upper() == 'PRIMARY KEY'
        result['unique'] = key.upper() in {'UNIQUE', 'UNIQUE KEY'}
        if key == 'FOREIGN KEY':
            result['related'] = {
                'to': constraint['references_table'],
                'by': self.get_columns(constraint['references_columns'])
            }
        result['sequence'] = 'auto_increment' in constraint
        result['related'] = None
        return result

    def get_constraint_type(self, type):
        return type.lower().replace('key', '').strip()

    def get_columns(self, columns):
        if columns is None:
            return None
        return columns.asList()

    def get_constraint_definition(self, constraint):
        result = {}

        result['name'] = constraint['name']
        result['type'] = self.get_constraint_type(constraint.get('type'))
        result['deferrable'] = constraint.get('deferrable', False)
        result['deferred'] = constraint.get('deferred', '').upper() == 'INITIALLY DEFERRED'
        result['deferrable'] = constraint.get('deferrable', '').upper() == 'DEFERRABLE'
        result['check'] = constraint.get('check', None)
        result['columns'] = self.get_columns(constraint.get('constraint_columns'))
        result['related_name'] = constraint.get('references_table', None)
        result['related_columns'] = self.get_columns(constraint.get('references_columns'))
        return result

    def parse_statement(self, sql=None):
        """
        Parse SQL into ZQL

        Arguments:
            sql: SQL statement, supports:
                - CREATE TABLE
                - CREATE INDEX (WIP)

        Return:
            ZQL object representing the CREATE TABLE statement
        """
        sql = sql or self.sql
        if not sql:
            raise ValueError('`sql` is not specified')

        parsed = self.PARSE.parseString(sql)
        result = {}
        if 'table' not in parsed:
            raise ValueError(f'failed to parse SQL: "{sql}"')
        table = parsed['table']
        schema = parsed.get('schema')
        result['name'] = f'{schema}.{table}' if schema else table
        result['temporary'] = "temporary" in parsed
        result['maybe'] = 'maybe' in parsed
        columns = []
        constraints = []

        for item in parsed["items"]:
            if item.getName() == "column":
                # add column
                # may have attached constraint
                columns.append(self.get_column_definition(item))

            elif item.getName() == "constraint":
                # add constraint
                constraints.append(self.get_constraint_definition(item))

        # use adbc.store.Table to update the constraints/columns
        name = table
        table = Table(name, backend=self, columns=columns, constraints=constraints)
        result['columns'] = named_dict_to_list(table.columns)
        result['constraints'] = named_dict_to_list(table.constraints)
        return {'create': {'table': result}}
