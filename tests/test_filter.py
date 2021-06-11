"""Tests for dframeio.filter"""

import pytest

from dframeio import filter


@pytest.mark.parametrize(
    "expression, expected_prefix_notation",
    [
        ("A > 5", "(> Column<A> 5)"),
        ("`A` > 5", "(> Column<A> 5)"),
        ("A >= 5", "(>= Column<A> 5)"),
        ("A < 5", "(< Column<A> 5)"),
        ("A <= 5", "(<= Column<A> 5)"),
        ("A != 5", "(!= Column<A> 5)"),
        ("A = 5.5", "(= Column<A> 5.5)"),
        ("A IS NULL", "(ISNULL Column<A>)"),
        ("A IS NOT NULL", "(NOTNULL Column<A>)"),
        ("A < B", "(< Column<A> Column<B>)"),
        ("A = 'xyz'", "(= Column<A> 'xyz')"),
        ('A = "xyz"', "(= Column<A> 'xyz')"),
        ("NOT A > 5", "(NOT (> Column<A> 5))"),
        ("NOT A > 5 AND B < 2", "(AND (NOT (> Column<A> 5)) (< Column<B> 2))"),
        ("NOT A > 5 OR B < 2", "(OR (NOT (> Column<A> 5)) (< Column<B> 2))"),
        ("a in ('RED','GREEN','BLUE')", "(IN Column<a> ['RED','GREEN','BLUE'])"),
        ("a in (10,20, 30)", "(IN Column<a> [10,20,30])"),
        ("table1.id = table2.id", "(= Column<table1.id> Column<table2.id>)"),
        (
            "a in ('RED','GREEN','BLUE') and b >2",
            "(AND (IN Column<a> ['RED','GREEN','BLUE']) (> Column<b> 2))",
        ),
        (
            "a in ('RED','GREEN','BLUE') and b.c in (10,20,30) OR d = 5",
            (
                "(OR "
                "    (AND (IN Column<a> ['RED','GREEN','BLUE']) "
                "         (IN Column<b.c> [10,20,30])) "
                "    (= Column<d> 5))"
            ),
        ),
    ],
)
def test_parse(expression, expected_prefix_notation):
    prefix = filter.to_prefix_notation(expression)
    assert prefix == expected_prefix_notation
