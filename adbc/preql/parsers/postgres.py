import re

FUNCTION_REGEX = re.compile(r'^([a-zA-Z][0-9a-zA-Z._]*)\(([^)]*)\)$')


class PostgresParser():
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
