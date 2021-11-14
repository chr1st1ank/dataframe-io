"""Tests for the handling of missing optional dependencies"""
import importlib
import sys


def test_missing_psycopg(monkeypatch):
    monkeypatch.setitem(sys.modules, "psycopg", None)
    import dframeio

    importlib.reload(dframeio)
    import dframeio.filter

    importlib.reload(dframeio.filter)
    import dframeio.abstract

    importlib.reload(dframeio.abstract)


def test_missing_pyarrow(monkeypatch):
    monkeypatch.setitem(sys.modules, "pyarrow", None)
    import dframeio

    importlib.reload(dframeio)
    import dframeio.filter

    importlib.reload(dframeio.filter)
    import dframeio.abstract

    importlib.reload(dframeio.abstract)
