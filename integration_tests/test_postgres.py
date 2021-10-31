"""Integration tests for the dframeio postgres implementation against a real server"""
# pylint: disable=redefined-outer-name
import os

import pandas as pd
import psycopg
import pytest

from dframeio import PostgresBackend

from .sample_data import SampleDataSet

CONNECTION_STRING = (
    "postgresql://user1:example@" + os.environ.get("POSTGRES_HOST", "localhost") + ":5432/user1"
)


def reset_test_table(refill=False):
    with psycopg.connect(conninfo=CONNECTION_STRING) as conn:
        with conn.cursor() as cursor:
            cursor.execute("DELETE FROM user1.testdb")
    if refill:
        PostgresBackend(conninfo=CONNECTION_STRING).write_append(
            "testdb", SampleDataSet().dataframe()
        )


@pytest.fixture()
def prepare_test_db():
    with psycopg.connect(conninfo=CONNECTION_STRING) as conn:
        with conn.cursor() as cursor:
            cursor.execute("DROP SCHEMA IF EXISTS user1 CASCADE")
            cursor.execute("CREATE SCHEMA user1")
            cursor.execute("CREATE TABLE user1.empty (col1 FLOAT, col2 VARCHAR)")
            cursor.execute(
                "CREATE TABLE user1.testdb (col_timedelta interval, "
                "col_datetime timestamp, col_bool boolean, "
                "col_int int, col_string varchar, col_float float)"
            )
    reset_test_table(refill=False)


@pytest.fixture()
def backend(prepare_test_db):
    return PostgresBackend(conninfo=CONNECTION_STRING)


@pytest.mark.parametrize("input_type", ["pandas", "dict"])
@pytest.mark.parametrize("input_mode", ["write_append", "write_replace"])
def test_write_read_roundtrip_pandas(backend, input_type, input_mode):
    """Write and then read the test data in a simple roundtrip"""
    reset_test_table(refill=False)
    sample_data = SampleDataSet()
    if input_type == "pandas":
        input_df = sample_data.dataframe()
    else:
        assert input_type == "dict"
        input_df = sample_data.datadict()
    getattr(backend, input_mode)(target="testdb", dataframe=input_df)
    output_data = backend.read_to_pandas("testdb")
    sample_data.assert_correct_and_equal(output_data)


#
# @pytest.mark.parametrize("input_mode", ["write_append", "write_replace"])
# def test_write_read_roundtrip_dict(backend, input_mode):
#     """Write and then read the test data in a simple roundtrip"""
#     reset_test_table(refill=False)
#     input_data = SampleDataSet()
#     backend.write_append(target="testdb", dataframe=input_data.dataframe())
#     output_data = backend.read_to_pandas("testdb")
#     input_data.assert_correct_and_equal(output_data)


@pytest.mark.parametrize(
    "kwargs, exception",
    [
        ({}, TypeError),  # source missing
        ({"columns": ["x"]}, TypeError),  # source missing
        ({"source": "table", "columns": "x"}, TypeError),  # columns must be iterable
        ({"source": "table", "row_filter": lambda x: x}, TypeError),  # row_filter must be string
        ({"source": "table", "row_filter": lambda x: x}, TypeError),  # row_filter must be string
        ({"source": "table", "limit": "x"}, TypeError),  # limit must be an int
        ({"source": "table", "limit": 2.5}, TypeError),  # limit must be an int
        ({"source": "table", "sample": "x"}, TypeError),  # sample must be an int
        ({"source": "table", "sample": 2.5}, TypeError),  # sample must be an int
    ],
)
def test_read_to_pandas_argchecks(backend: PostgresBackend, kwargs, exception):
    """Challenge the argument validation of the constructor"""
    with pytest.raises(exception):
        backend.read_to_pandas(**kwargs)


def test_read_to_pandas_empty_table(backend: PostgresBackend):
    df = backend.read_to_pandas("empty")
    pd.testing.assert_frame_equal(df, pd.DataFrame(columns=["col1", "col2"]))


def test_read_to_dict_empty_table(backend: PostgresBackend):
    df = backend.read_to_dict("empty")
    assert df == dict(col1=[], col2=[])


#
# abstract method
# read_to_pandas(source, columns=None, row_filter=None, limit=-1, sample=-1, drop_duplicates=False)

#
# def test_read_to_pandas_all(backend: PostgresBackend):
#     """Is the full dataset correctly read?"""
#     df = backend.read_to_pandas("testdb")
#     SampleDataSet().assert_correct_and_equal(df)


# def test_read_tables(backend: PostgresBackend):
#     df = backend.read_to_pandas("information_schema.tables")
#     print(df)
#     assert False
