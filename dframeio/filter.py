"""Filter expressions for data reading operations with predicate pushdown.

This module is responsible for translate filter expressions from a simplified SQL syntax
into different formats understood by the various backends. This way the same language can
be used to implement filtering regardless of the data source.

The grammar of the filter statements is the same as in a WHERE clause in SQL. Supported
features:

  - Comparing column values to numbers, strings and another column's values using the operators
    `> < = != >= <=`, e.g. `a.column < 5`
  - Comparison against a set of values with ÌN and `NOT IN`, e.g. `a.column IN (1, 2, 3)`
  - Boolean combination of conditions with `AND`, `OR` and `ǸOT`
  - `NULL` comparison as in `a IS NULL` or `b IS NOT NULL`

Strings can be quoted with single-quotes and double-quotes.
Column names can but don't have to be quoted with SQL quotes (backticks). E.g.:

```sql
`a.column` = "abc" AND b IS NOT NULL OR index < 50
```
"""
from dataclasses import dataclass
from typing import Any, List, Tuple, Type, Union

import lark

# See SQL 2003 working draft http://www.wiscorp.com/sql20nn.zip (Part 2, section 6.35)
lark_grammar = r"""
    ?boolean_value_exp: boolean_term
        | boolean_value_exp "OR"i boolean_term -> or_operation
    ?boolean_term: boolean_factor
        | boolean_term "AND"i boolean_factor -> and_operation
    ?boolean_factor:  "(" boolean_value_exp ")"
        | single_condition
        | "NOT" single_condition -> negation
    ?single_condition: comparison
        | null_comparison
        | notin_list
        | in_list
    comparison: columnname BINOP column_rval
    null_comparison: columnname (ISNULL | NOTNULL)
    notin_list: columnname NOTIN literal_list
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
    NOTIN.10: /NOT\s+IN/i
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


def _make_parser(transformer: Type[lark.Transformer]):
    return lark.Lark(
        lark_grammar,
        start="boolean_value_exp",
        lexer="standard",
        parser="lalr",
        transformer=transformer,
    )


class _PrefixNotationTransformer(lark.Transformer):
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
    def notin_list(self, column: lark.Token, notin: lark.Token, lst: lark.Token):
        return self.format_operation("NOTIN", column, lst)

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


def to_prefix_notation(statement: str) -> str:
    """Parse a filter statement and return it in prefix notation.

    Args:
        statement: A filter predicate as string

    Returns:
        The filter statement in prefix notation (polish notation) as string

    Examples:
        >>> to_prefix_notation("a.column != 0")
        '(!= Column<a.column> 0)'

        >>> to_prefix_notation("a > 1 and b <= 3")
        '(AND (> Column<a> 1) (<= Column<b> 3))'
    """
    parser = _make_parser(_PrefixNotationTransformer())
    return parser.parse(statement)


class _PyarrowDNFTransformer(lark.Transformer):
    @dataclass
    class Column:
        name: str

    @dataclass
    class Or:
        operands: list

        def format(self) -> List[List[Tuple]]:
            disjunction = []
            for o in self.operands:
                if isinstance(o, _PyarrowDNFTransformer.And):
                    disjunction.append(o.format())
                elif isinstance(o, _PyarrowDNFTransformer.Condition):
                    disjunction.append([o.format()])
                else:
                    raise ValueError(
                        "For PyArrow only disjunctions of conjunctions "
                        "or simple conditions are allowed. "
                        f"`{o}` is not a single condition nor a conjunction."
                    )
            return disjunction

    @dataclass
    class And:
        operands: list

        def format(self) -> List[Tuple]:
            conjunction = []
            for o in self.operands:
                if isinstance(o, _PyarrowDNFTransformer.Condition):
                    conjunction.append(o.format())
                else:
                    raise ValueError(
                        "For PyArrow only conjunctions of simple conditions are allowed."
                    )
            return conjunction

    @dataclass
    class Condition:
        key: str
        operator: str
        value: Union[str, int, float]

        def format(self) -> Tuple:
            if not isinstance(self.key, _PyarrowDNFTransformer.Column):
                raise ValueError(
                    "For pyarrow only comparisons of the form `key <operator> value`"
                    "are allowed, where `key` must be a column name."
                )
            if not isinstance(self.value, (int, float, str, set)):
                raise ValueError(
                    "For pyarrow only comparisons of the form `key <operator> value` are allowed."
                )
            return self.key.name, self.operator, self.value

    def and_operation(self, operands: lark.Token):
        return _PyarrowDNFTransformer.And(list(operands))

    def or_operation(self, operands: lark.Token):
        return _PyarrowDNFTransformer.Or(list(operands))

    @lark.v_args(inline=True)
    def comparison(self, left: lark.Token, operator: lark.Token, right: lark.Token):
        return _PyarrowDNFTransformer.Condition(left, operator.value, right)

    @lark.v_args(inline=True)
    def null_comparison(self, operand: lark.Token, operator: lark.Token):
        if operator.type == "ISNULL":
            return _PyarrowDNFTransformer.Condition(operand, "=", "null")
        if operator.type == "NOTNULL":
            return _PyarrowDNFTransformer.Condition(operand, "!=", "null")
        raise lark.ParseError("Invalid NULL comparison")

    def negation(self, _):
        raise ValueError("Pyarrow doesn't support the `NOT` operator")

    @lark.v_args(inline=True)
    def notin_list(self, column: lark.Token, notin: lark.Token, lst: lark.Token):
        return _PyarrowDNFTransformer.Condition(column, "not in", lst)

    @lark.v_args(inline=True)
    def in_list(self, column: lark.Token, lst: lark.Token):
        return _PyarrowDNFTransformer.Condition(column, "in", lst)

    def literal_list(self, members: lark.Token):
        return set(members)

    def UNQUOTED_COLUMNNAME(self, name: lark.Token):
        return _PyarrowDNFTransformer.Column(name.value)

    def ESCAPED_STRING(self, tok: lark.Token):
        assert len(tok) > 1 and (tok[0] == tok[-1] == "'" or tok[0] == tok[-1] == '"')
        return tok[1:-1]

    def SIGNED_NUMBER(self, tok: lark.Token):
        """Convert the value of `tok` from string to number"""
        try:
            return int(tok)
        except ValueError:
            return float(tok)


def _raise_error(e: Exception):
    raise e


def to_pyarrow_dnf(statement: str) -> List[List[Tuple[str, str, Any]]]:
    """Convert a filter statement to the disjunctive normal form understood by pyarrow

    Predicates are expressed in disjunctive normal form (DNF), like `[[('x', '=', 0), ...], ...]`.
    The outer list is understood as chain of disjunctions ("or"), every inner list as a chain
    of conjunctions ("and"). The inner lists contain tuples with a single operation
    in infix notation each.
    More information about the format and its limitations can be found in the
    [pyarrow documentation](https://arrow.apache.org/docs/python/generated/pyarrow.parquet.read_table.html#pyarrow-parquet-read-table).

    Args:
        statement: A filter predicate as string

    Returns:
        The filter statement converted to a list of lists of tuples.

    Examples:
        >>> to_pyarrow_dnf("a.column != 0")
        [[('a.column', '!=', 0)]]

        >>> to_pyarrow_dnf("a > 1 and b <= 3")
        [[('a', '>', 1), ('b', '<=', 3)]]

        >>> to_pyarrow_dnf("a > 1 and b <= 3 or c = 'abc'")
        [[('a', '>', 1), ('b', '<=', 3)], [('c', '=', 'abc')]]
    """  # noqa: E501
    parser = _make_parser(_PyarrowDNFTransformer())
    predicate = parser.parse(statement, on_error=_raise_error)
    if isinstance(predicate, _PyarrowDNFTransformer.Condition):
        return [[predicate.format()]]
    if isinstance(predicate, _PyarrowDNFTransformer.And):
        return [predicate.format()]
    if isinstance(predicate, _PyarrowDNFTransformer.Or):
        return predicate.format()
    RuntimeError("Invalid statement")


class _SQLTransformer(lark.Transformer):
    def SIGNED_NUMBER(self, tok):
        """Convert the value of `tok` from string to number"""
        try:
            return tok.update(value=int(tok))
        except ValueError:
            return tok.update(value=float(tok))

    def ESCAPED_STRING(self, tok):
        assert len(tok) > 1 and (tok[0] == tok[-1] == "'" or tok[0] == tok[-1] == '"')
        return tok.update(value=tok[1:-1])
