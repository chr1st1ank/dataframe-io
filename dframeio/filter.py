"""Filter expressions for predicate pushdown"""
import logging
from typing import Type

import lark

# See SQL 2003 working draft http://www.wiscorp.com/sql20nn.zip (Part 2, section 6.35)
lark_grammar = r"""
    ?predicate: "(" predicate ")"
        | boolean_value_exp
    ?boolean_value_exp: boolean_term
        | boolean_value_exp "OR"i boolean_term -> or_operation
    ?boolean_term: boolean_factor
        | boolean_term "AND"i boolean_factor -> and_operation
    ?boolean_factor: single_condition
        | "NOT" single_condition -> negation
    ?single_condition: comparison
        | null_comparison
        | in_list
    comparison: columnname BINOP column_rval
    null_comparison: columnname (ISNULL | NOTNULL)
    in_list: columnname "IN"i literal_list
    literal_list: ("(") literal ("," literal)* (")")
    ?literal: SIGNED_NUMBER | ESCAPED_STRING
    ?column_rval: columnname
        | SIGNED_NUMBER
        | ESCAPED_STRING
    ?columnname: "`" columnname "`"
        | UNQUOTED_COLUMNNAME

    ISNULL.9: /IS\s+NULL/i
    NOTNULL.10: /IS\s+NOT\s+NULL/i
    NOT.9: "NOT"i
    UNQUOTED_COLUMNNAME.1: NAMECHAR+ ("." + NAMECHAR+)*
    NAMECHAR: "_"|"$"|LETTER|DIGIT
    BINOP.10: "!="
         | ">="
         | "<="
         | "<"
         | ">"
         | "="
    ESCAPED_STRING.2 : DOUBLE_QUOTE_ESCAPED_STRING | SINGLE_QUOTE_ESCAPED_STRING
    DOUBLE_QUOTE_ESCAPED_STRING.2 : "\"" _STRING_ESC_INNER "\""
    SINGLE_QUOTE_ESCAPED_STRING.2 : "'" _STRING_ESC_INNER "'"

    %import common.LETTER
    %import common.DIGIT
    %import common.SIGNED_NUMBER
    %import common._STRING_ESC_INNER
    %import common.WS_INLINE
    %ignore WS_INLINE
"""


class BaseTransformer(lark.Transformer):
    def SIGNED_NUMBER(self, tok):
        """Convert the value of `tok` from string to number"""
        try:
            return tok.update(value=int(tok))
        except ValueError:
            return tok.update(value=float(tok))

    def ESCAPED_STRING(self, tok):
        assert len(tok) > 1 and (tok[0] == tok[-1] == "'" or tok[0] == tok[-1] == '"')
        return tok.update(value=tok[1:-1])


class PrefixNotationTransformer(lark.Transformer):
    def and_operation(self, operands: lark.Token):
        return self.format_operation("AND", *operands)

    def or_operation(self, operands: lark.Token):
        return self.format_operation("OR", *operands)

    @lark.v_args(inline=True)
    def comparison(self, left: lark.Token, operator: lark.Token, right: lark.Token):
        return self.format_operation(operator, left, right)

    @lark.v_args(inline=True)
    def null_comparison(self, operand: lark.Token, operator: lark.Token):
        if operator.type == "ISNULL":
            return self.format_operation("ISNULL", operand)
        if operator.type == "NOTNULL":
            return self.format_operation("NOTNULL", operand)
        raise lark.ParseError("Invalid NULL comparison")

    @lark.v_args(inline=True)
    def negation(self, operand: lark.Token):
        return self.format_operation("NOT", operand)

    @lark.v_args(inline=True)
    def in_list(self, column: lark.Token, lst: lark.Token):
        return self.format_operation("IN", column, lst)

    def literal_list(self, members: lark.Token):
        return "[" + ",".join(m for m in members) + "]"

    def UNQUOTED_COLUMNNAME(self, name: lark.Token):
        return f"Column<{name.value}>"

    def ESCAPED_STRING(self, tok: lark.Token):
        assert len(tok) > 1 and (tok[0] == tok[-1] == "'" or tok[0] == tok[-1] == '"')
        return "'" + tok[1:-1] + "'"

    def SIGNED_NUMBER(self, tok: lark.Token):
        """Convert the value of `tok` from string to number"""
        try:
            return str(int(tok))
        except ValueError:
            return str(float(tok))

    @staticmethod
    def format_operation(operator, *operands):
        return f"({operator} {' '.join([o for o in operands])})"


def _make_parser(transformer: Type[lark.Transformer]):
    return lark.Lark(
        lark_grammar,
        start="predicate",
        lexer="standard",
        parser="lalr",
        transformer=transformer,
    )


def to_prefix_notation(statement: str) -> str:
    """Parse a filter statement and return it in prefix notation

    Args:
        statement: A filter predicate as string

    Returns:
        The filter statement in prefix notation (polish notation) as string
    """
    parser = _make_parser(PrefixNotationTransformer())
    return parser.parse(statement)


if __name__ == "__main__":

    examples = """\
        a >0 and b <= 2
        a in ('RED','GREEN','BLUE') and b in (10,20,30)
        a in ('RED','GREEN','BLUE') and b.c in (10,20,30) OR d = 5
        a in ('RED','GREEN','BLUE')
        table1.id = table2.id
        A > 5
        `A` > 5
        A >= 5
        A < 5
        A <= 5
        A != 5
        A IS NULL
        A IS NOT NULL
        A < B
        A = "xyz"
        NOT A > 5\
        """.splitlines()

    lark.logger.setLevel(logging.DEBUG)
    p = lark.Lark(
        lark_grammar,
        start="predicate",
        lexer="standard",
        parser="lalr",
        # transformer=BaseTransformer(),
        debug=True,
    )
    for i, e in enumerate(reversed(examples)):
        print(">> ", e)
        parsed = p.parse(e)
        print(parsed.pretty())
        # tree.pydot__tree_to_png(parsed, f"sppf-{i}.png")
        print(PrefixNotationTransformer().transform(parsed))
        print("--")
