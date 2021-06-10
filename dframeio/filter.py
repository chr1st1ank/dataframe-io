"""Filter expressions for predicate pushdown"""
import logging

from lark import Lark, logger, tree

examples = """\
    a in ('RED','GREEN','BLUE') and b in (10,20,30)
    a in ('RED','GREEN','BLUE') and b.c in (10,20,30) OR d = 5
    a in ('RED','GREEN','BLUE')
    table1.id = table2.id
    A > 5
    A >= 5
    A < 5
    A <= 5
    A != 5
    A IS NULL
    A IS NOT NULL
    A < B
    A = "xyz"\
    """.splitlines()

lark_grammar = r"""
    predicate: "(" predicate ")"
        | single_condition
        | and_expr
        | or_expr
    and_expr: predicate "AND"i predicate
    or_expr: predicate "OR"i predicate
    ?single_condition: comparison
        | null_comparison
        | in_list
    comparison: COLUMNNAME BINOP column_rval
    null_comparison: COLUMNNAME (ISNULL | NOTNULL)
    in_list: COLUMNNAME "IN"i literal_list
    literal_list: ("(") literal ("," literal)* (")")
    ?literal: SIGNED_NUMBER | ESCAPED_STRING
    ?column_rval: COLUMNNAME
        | SIGNED_NUMBER
        | ESCAPED_STRING

    ISNULL.9: /IS\s+NULL/i
    NOTNULL.10: /IS\s+NOT\s+NULL/i
    COLUMNNAME.1: NAMECHAR+ ("." + NAMECHAR+)*
    NAMECHAR: "_"|"$"|LETTER|DIGIT
    BINOP.10: "!="
         | ">="
         | "<="
         | "<"
         | ">"
         | "="
    ESCAPED_STRING.2 : DOUBLE_QUOTE_ESCAPED_STRING | SINGLE_QUOTE_ESCAPED_STRING
    DOUBLE_QUOTE_ESCAPED_STRING.2 : "\\"" _STRING_ESC_INNER "\\""
    SINGLE_QUOTE_ESCAPED_STRING.2 : "'" _STRING_ESC_INNER "'"

    %import common.LETTER
    %import common.DIGIT
    %import common.SIGNED_NUMBER
    %import common._STRING_ESC_INNER
    %import common.WS_INLINE
    %ignore WS_INLINE
"""

logger.setLevel(logging.DEBUG)
p = Lark(lark_grammar, start="predicate", lexer="standard", parser="lalr", debug=True)
for i, e in enumerate(reversed(examples)):
    print(">> ", e)
    parsed = p.parse(e)
    print(parsed)
    tree.pydot__tree_to_png(parsed, f"sppf-{i}.png")
    print("--")
