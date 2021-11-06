"""Unit tests for the postgres backend which work without database"""
# pylint: disable=redefined-outer-name

import psycopg
import pytest

from dframeio import postgres


class FakeConnection:
    def __init__(self, data):
        self.data = data

    def connect(self, *args, **kwargs):
        return self

    def cursor(self):
        return self

    # def execute(self, *args, **kwargs):
    #     return True
    # def description(self):
    #     """Return list of column names"""
    #     return self.data.columns


CONNECTION_STRING = "postgresql://user1:example@dbserver:5432"


@pytest.mark.parametrize(
    "kwargs, exception",
    [
        ({}, TypeError),  # source missing
        ({"columns": ["x"]}, TypeError),  # source missing
        ({"source": "table", "columns": 5}, TypeError),  # columns must be a list of column names
        ({"source": "table", "columns": "x"}, TypeError),  # columns must be a list of column names
        ({"source": "table", "row_filter": lambda x: x}, TypeError),  # row_filter must be string
        ({"source": "table", "row_filter": lambda x: x}, TypeError),  # row_filter must be string
        ({"source": "table", "limit": "x"}, TypeError),  # limit must be an int
        ({"source": "table", "limit": 2.5}, TypeError),  # limit must be an int
        ({"source": "table", "sample": "x"}, TypeError),  # sample must be an int
        ({"source": "table", "sample": 2.5}, TypeError),  # sample must be an int
    ],
)
def test_read_to_pandas_argchecks(monkeypatch, kwargs, exception):
    """Challenge the argument validation of the constructor"""
    mock = FakeConnection(None)
    monkeypatch.setattr(psycopg, "connect", mock.connect)

    backend = postgres.PostgresBackend(conninfo=CONNECTION_STRING)
    with pytest.raises(exception):
        backend.read_to_pandas(**kwargs)
