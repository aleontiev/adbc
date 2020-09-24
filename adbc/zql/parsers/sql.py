import re


from pyparsing import (
    CaselessKeyword, Forward, Word, Regex, alphanums,
    delimitedList, Suppress, Optional, Group, OneOrMore
)

FUNCTION_REGEX = re.compile(r'^([a-zA-Z][0-9a-zA-Z._]*)\(([^)]*)\)$')


class SQLParser(ParserBase):
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

    _LPAR, _RPAR, _COMMA, _SEMICOLON, _DOT, _DOUBLEQUOTE, _BACKQUOTE, _SPACE = map(Suppress, "(),;.\"` ")
    _CREATE, _TABLE, _TEMP, _CONSTRAINT, _NOT_NULL, _PRIMARY_KEY, _UNIQUE, _UNIQUE_KEY, _FOREIGN_KEY, _REFERENCES, _KEY, _CHAR_SEMANTICS, _BYTE_SEMANTICS = \
        map(CaselessKeyword, "CREATE, TABLE, TEMP, CONSTRAINT, NOT NULL, PRIMARY KEY, UNIQUE, UNIQUE KEY, FOREIGN KEY, REFERENCES, KEY, CHAR, BYTE".replace(", ", ",").split(","))
    _TYPE_UNSIGNED, _TYPE_ZEROFILL = \
        map(CaselessKeyword, "UNSIGNED, ZEROFILL".replace(", ", ",").split(","))
    _COL_ATTR_DISTKEY, _COL_ATTR_SORTKEY, _COL_ATTR_CHARACTER_SET = \
        map(CaselessKeyword, "DISTKEY, SORTKEY, CHARACTER SET".replace(", ", ",").split(","))
    _FK_MATCH = \
        CaselessKeyword("MATCH") + Word(alphanums + "_")
    _FK_ON, _FK_ON_OPT_RESTRICT, _FK_ON_OPT_CASCADE, _FK_ON_OPT_SET_NULL, _FK_ON_OPT_NO_ACTION = \
        map(CaselessKeyword, "ON, RESTRICT, CASCADE, SET NULL, NO ACTION".replace(", ", ",").split(","))
    _FK_ON_DELETE = \
        _FK_ON + CaselessKeyword("DELETE") + (_FK_ON_OPT_RESTRICT | _FK_ON_OPT_CASCADE | _FK_ON_OPT_SET_NULL | _FK_ON_OPT_NO_ACTION)
    _FK_ON_UPDATE = \
        _FK_ON + CaselessKeyword("UPDATE") + (_FK_ON_OPT_RESTRICT | _FK_ON_OPT_CASCADE | _FK_ON_OPT_SET_NULL | _FK_ON_OPT_NO_ACTION)
    _SUPPRESS_QUOTE = _BACKQUOTE | _DOUBLEQUOTE

    _COMMENT = Suppress("--" + Regex(r".+"))


    _CREATE_TABLE_STATEMENT = Suppress(_CREATE) + Optional(_TEMP)("temporary") + Suppress(_TABLE) + Optional(Optional(CaselessKeyword("IF NOT EXISTS")("maybe"))) \
        + Optional(_SUPPRESS_QUOTE) + Optional(Word(alphanums + "_")("schema") + Optional(_SUPPRESS_QUOTE) + _DOT + Optional(_SUPPRESS_QUOTE)) + Word(alphanums + "_<>")("table") + Optional(_SUPPRESS_QUOTE) \
        + _LPAR \
        + delimitedList(
            OneOrMore(
                _COMMENT
                |
                # Ignore Index
                Suppress(_KEY + Word(alphanums + "_'`() "))
                |
                Group(
                    Optional(Suppress(_CONSTRAINT) + Optional(_SUPPRESS_QUOTE) + Word(alphanums + "_")("name") + Optional(_SUPPRESS_QUOTE))
                    + (
                        (
                            (_PRIMARY_KEY ^ _UNIQUE ^ _UNIQUE_KEY ^ _NOT_NULL)("type")
                            + Optional(_SUPPRESS_QUOTE) + Optional(Word(alphanums + "_"))("name") + Optional(_SUPPRESS_QUOTE)
                            + _LPAR + Group(delimitedList(Optional(_SUPPRESS_QUOTE) + Word(alphanums + "_") + Optional(_SUPPRESS_QUOTE)))("constraint_columns") + _RPAR
                        )
                        |
                        (
                            (_FOREIGN_KEY)("type")
                            + _LPAR + Group(delimitedList(Optional(_SUPPRESS_QUOTE) + Word(alphanums + "_") + Optional(_SUPPRESS_QUOTE)))("constraint_columns") + _RPAR
                            + Optional(Suppress(_REFERENCES)
                                + Optional(_SUPPRESS_QUOTE) + Word(alphanums + "_")("references_table") + Optional(_SUPPRESS_QUOTE)
                                + _LPAR + Group(delimitedList(Optional(_SUPPRESS_QUOTE) + Word(alphanums + "_") + Optional(_SUPPRESS_QUOTE)))("references_columns") + _RPAR
                                + Optional(_FK_MATCH)("references_fk_match")  # MySQL
                                + Optional(_FK_ON_DELETE)("references_fk_on_delete")  # MySQL
                                + Optional(_FK_ON_UPDATE)("references_fk_on_update")  # MySQL
                            )
                        )
                    )
                )("constraint")
                |
                Group(
                    ((_SUPPRESS_QUOTE + Word(alphanums + " _")("name") + _SUPPRESS_QUOTE) ^ (Optional(_SUPPRESS_QUOTE) + Word(alphanums + "_")("name") + Optional(_SUPPRESS_QUOTE)))
                    + Group(
                        Group(
                            Word(alphanums + "_")
                            + Optional(CaselessKeyword("WITHOUT TIME ZONE") ^ CaselessKeyword("WITH TIME ZONE") ^ CaselessKeyword("PRECISION") ^ CaselessKeyword("VARYING"))
                        )("type_name")
                        + Optional(_LPAR + Regex(r"[\d\*]+\s*,*\s*\d*")("length") + Optional(_CHAR_SEMANTICS | _BYTE_SEMANTICS)("semantics") + _RPAR)
                        + Optional(_TYPE_UNSIGNED)("unsigned")
                        + Optional(_TYPE_ZEROFILL)("zerofill")
                    )("type")
                    + Optional(Word(r"\[\]"))("array_brackets")
                    + Optional(
                        Regex(r"(?!--)", re.IGNORECASE)
                        + Group(
                            Optional(Regex(r"\b(?:NOT\s+)NULL?\b", re.IGNORECASE))("null")
                            & Optional(Regex(r"\bAUTO_?INCREMENT\b", re.IGNORECASE))("auto_increment")
                            & Optional(Regex(r"\b(UNIQUE|PRIMARY)(?:\s+KEY)?\b", re.IGNORECASE))("key")
                            & Optional(Regex(
                                r"\bDEFAULT\b\s+(?:((?:[A-Za-z0-9_\.\'\" -\{\}]|[^\x01-\x7E])*\:\:(?:character varying)?[A-Za-z0-9\[\]]+)|(?:\')((?:\\\'|[^\']|,)+)(?:\')|(?:\")((?:\\\"|[^\"]|,)+)(?:\")|([^,\s]+))",
                                re.IGNORECASE))("default")
                            & Optional(Regex(r"\bCOMMENT\b\s+(\'(\\\'|[^\']|,)+\'|\"(\\\"|[^\"]|,)+\"|[^,\s]+)", re.IGNORECASE))("comment")
                            & Optional(Regex(r"\bENCODE\s+[A-Za-z0-9]+\b", re.IGNORECASE))("encode")  # Redshift
                            & Optional(_COL_ATTR_DISTKEY)("distkey")  # Redshift
                            & Optional(_COL_ATTR_SORTKEY)("sortkey")  # Redshift
                            & Optional(Suppress(_COL_ATTR_CHARACTER_SET) + Word(alphanums + "_")("character_set"))  # MySQL
                        )("constraint")
                    )
                )("column")
                |
                _COMMENT
            )
        )("columns")

    _PARSE_EXPRESSION = Forward()
    _PARSE_EXPRESSION << OneOrMore(_COMMENT | _CREATE_TABLE_STATEMENT)


    def __init__(self, sql=None):
        self.sql = sql

    def remove_cast(self, literal: str):
        if '::' in literal:
            literal = literal.split('::')[0]
        return literal

    def parse_literal(self, literal: str):
        return self.remove_cast(literal)

    def parse(self, expression: str):
        # TODO: get a real SQL parser
        # this is super hacky
        if expression is None:
            return expression

        if '(' in expression:
            match = FUNCTION_REGEX.match(expression)
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

    def parse2(self, sql=None):
        """
        Parse DDL script.

        Arguments:
            sql: SQL statement, supports:
                - CREATE TABLE
                - CREATE INDEX

        Return:
            ZQL object representing the CREATE TABLE statement
        """
        sql = sql or self.sql
        if not sql:
            raise ValueError('`sql` is not specified')

        parsed = self._PARSE_EXPRESSION.parseString(sql)
        result = {}
        schema = parsed['schema']
        table = parsed['name']
        result['name'] = f'{schema}.{table}' if schema else table
        result['temporary'] = "temporary" in parsed
        result['maybe'] = 'maybe' in parsed
        result['columns'] = []

        for column in parsed["columns"]:
            if column.getName() == "column":
                # add column
                column = result['columns'].append(
                    self.get_column(column)
                )

            elif column.getName() == "constraint":
                # set column constraint
                for constraint_column in column["constraint_columns"]:
                    constraint_column = result['columns'][constraint_column]

                    if column["type"] == "PRIMARY KEY":
                        constraint_column['null'] = False
                        constraint_column['primary'] = True
                    elif column["type"] in ["UNIQUE", "UNIQUE KEY"]:
                        constraint_column['unique'] = True
                    elif column["type"] == "NOT NULL":
                        constraint_column['null'] = False

        return result
