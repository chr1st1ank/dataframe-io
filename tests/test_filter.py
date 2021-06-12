"""Tests for dframeio.filter"""
import doctest

import pytest

from dframeio import filter


def test_doctest():
    try:
        doctest.testmod(filter, raise_on_error=True, verbose=True)
    except doctest.DocTestFailure as f:
        print(f"Got:\n    {f.got}")
        assert f.example.want.strip() == f.got.strip(), "Doctest failed"


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
        ("a not in ('RED','GREEN','BLUE')", "(NOTIN Column<a> ['RED','GREEN','BLUE'])"),
        ("a in (10,20, 30)", "(IN Column<a> [10,20,30])"),
        ("table1.id = table2.id", "(= Column<table1.id> Column<table2.id>)"),
        ("a < 1 OR b > 2", "(OR (< Column<a> 1) (> Column<b> 2))"),
        ("a < 1 OR b > 2 AND c=0", "(OR (< Column<a> 1) (AND (> Column<b> 2) (= Column<c> 0)))"),
        ("(a < 1 OR b > 2) AND c=0", "(AND (OR (< Column<a> 1) (> Column<b> 2)) (= Column<c> 0))"),
        (
            "a in ('RED','GREEN','BLUE') and b >2",
            "(AND (IN Column<a> ['RED','GREEN','BLUE']) (> Column<b> 2))",
        ),
        (
            "a in ('RED','GREEN','BLUE') and b.c in (10,20,30) OR d = 5",
            (
                "(OR"
                " (AND (IN Column<a> ['RED','GREEN','BLUE'])"
                " (IN Column<b.c> [10,20,30])) "
                "(= Column<d> 5))"
            ),
        ),
    ],
)
def test_to_prefix_notation(expression, expected_prefix_notation):
    prefix = filter.to_prefix_notation(expression)
    assert prefix == expected_prefix_notation


@pytest.mark.parametrize(
    "expression, expected_dnf",
    [
        ("A > 5", [[("A", ">", 5)]]),
        ("`A` > 5", [[("A", ">", 5)]]),
        ("A >= 5", [[("A", ">=", 5)]]),
        ("A < 5", [[("A", "<", 5)]]),
        ("A <= 5", [[("A", "<=", 5)]]),
        ("A != 5", [[("A", "!=", 5)]]),
        ("A = 5.5", [[("A", "=", 5.5)]]),
        ("A IS NULL", [[("A", "=", "null")]]),
        ("A IS NOT NULL", [[("A", "!=", "null")]]),
        ("A = 'xyz'", [[("A", "=", "xyz")]]),
        ('A = "xyz"', [[("A", "=", "xyz")]]),
        ("a in ('RED','GREEN','BLUE')", [[("a", "in", {"RED", "GREEN", "BLUE"})]]),
        ("a not in ('RED','GREEN','BLUE')", [[("a", "not in", {"RED", "GREEN", "BLUE"})]]),
        ("a in (10,20, 30)", [[("a", "in", {10, 20, 30})]]),
        (
            "a in ('RED','GREEN','BLUE') and b >2",
            [[("a", "in", {"RED", "GREEN", "BLUE"}), ("b", ">", 2)]],
        ),
        ("a < 1 OR b > 2", [[("a", "<", 1)], [("b", ">", 2)]]),
        ("a < 1 OR b > 2 AND c=0", [[("a", "<", 1)], [("b", ">", 2), ("c", "=", 0)]]),
        (
            "a in ('RED','GREEN','BLUE') and b.c in (10,20,30) OR d = 5",
            [[("a", "in", {"BLUE", "GREEN", "RED"}), ("b.c", "in", {10, 20, 30})], [("d", "=", 5)]],
        ),
    ],
)
def test_to_pyarrow_dnf(expression, expected_dnf):
    dnf = filter.to_pyarrow_dnf(expression)
    assert dnf == expected_dnf


@pytest.mark.parametrize(
    "expression",
    [
        "A < B",
        "NOT A > 5",
        "table1.id = table2.id",
        "a in ('RED','GREEN','BLUE') and (b.c in (10,20,30) OR d = 5)",
    ],
)
def test_to_pyarrow_dnf_errors(expression):
    with pytest.raises(ValueError):
        print(filter.to_pyarrow_dnf(expression))
