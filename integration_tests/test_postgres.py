"""Integration tests for the dframeio postgres implementation against a real server"""
# pylint: disable=redefined-outer-name
import os

import pandas as pd
import psycopg
import pytest

from dframeio import PostgresBackend

from .sample_data import SampleDataSchema, SampleDataSet

CONNECTION_STRING = (
    "postgresql://user1:example@" + os.environ.get("POSTGRES_HOST", "localhost") + ":5432/user1"
)


def reset_test_table(refill: bool = False):
    """Empty the test db table user1.testdb and maybe refill it with sample data

    Args:
        refill: Whether to fill the emptied table with sample data again
    """
    with psycopg.connect(conninfo=CONNECTION_STRING) as conn:
        with conn.cursor() as cursor:
            cursor.execute("DELETE FROM user1.testdb")
    if refill:
        PostgresBackend(conninfo=CONNECTION_STRING).write_append(
            "testdb", SampleDataSet().dataframe()
        )


@pytest.fixture()
def prepare_test_db():
    """Set up schema and tables for the test database"""
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
def backend(prepare_test_db) -> PostgresBackend:
    """Initialized PostgresBackend pointing to the test database."""
    return PostgresBackend(conninfo=CONNECTION_STRING)


@pytest.mark.parametrize("input_mode", ["write_append", "write_replace"])
@pytest.mark.parametrize("input_type", ["pandas", "dict"])
@pytest.mark.parametrize("output_type", ["pandas", "dict"])
def test_write_read_roundtrip(backend, input_type, input_mode, output_type):
    """Write and then read the test data in a simple roundtrip"""
    reset_test_table(refill=False)
    sample_data = SampleDataSet()
    if input_type == "pandas":
        input_df = sample_data.dataframe()
    else:
        assert input_type == "dict"
        input_df = sample_data.datadict()
    getattr(backend, input_mode)(target="testdb", dataframe=input_df)
    if input_type == "pandas":
        output_data = backend.read_to_pandas("testdb")
    else:
        assert input_type == "dict"
        output_data = backend.read_to_dict("testdb")
    sample_data.assert_correct_and_equal(output_data)


@pytest.mark.parametrize("input_mode", ["write_append", "write_replace"])
def test_write_read_roundtrip_dict(backend, input_mode):
    """Write and then read the test data in a simple roundtrip"""
    reset_test_table(refill=False)
    input_data = SampleDataSet()
    backend.write_append(target="testdb", dataframe=input_data.dataframe())
    output_data = backend.read_to_pandas("testdb")
    input_data.assert_correct_and_equal(output_data)


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
def test_read_to_pandas_argchecks(backend: PostgresBackend, kwargs, exception):
    """Challenge the argument validation of the constructor"""
    with pytest.raises(exception):
        backend.read_to_pandas(**kwargs)


def test_read_to_pandas_empty_table(backend: PostgresBackend):
    """Try reading an empty table and see if the column names are fetched"""
    df = backend.read_to_pandas("empty")
    pd.testing.assert_frame_equal(df, pd.DataFrame(columns=["col1", "col2"]))


def test_read_to_dict_empty_table(backend: PostgresBackend):
    """Try reading an empty table and see if the column names are fetched"""
    df = backend.read_to_dict("empty")
    assert df == dict(col1=[], col2=[])


@pytest.mark.parametrize("df_type", [pd.DataFrame, dict])
def test_read_columns(df_type, backend: PostgresBackend):
    """Is the full dataset correctly read?"""
    reset_test_table(True)
    df = backend.read_to_pandas("testdb", columns=["col_int", "col_string"])
    SampleDataSet().select_columns(columns=["col_int", "col_string"]).assert_correct_and_equal(df)


def test_read_to_dict_columns(backend: PostgresBackend):
    """Is the full dataset correctly read?"""
    reset_test_table(True)
    df = backend.read_to_dict("testdb", columns=["col_int", "col_string"])
    SampleDataSet().select_columns(columns=["col_int", "col_string"]).assert_correct_and_equal(df)


@pytest.mark.parametrize(
    "filter_expr, expected_data",
    [
        ("col_int >= 3", SampleDataSet().where_int_greater_equal_3()),
        ("col_string IS NOT NULL", SampleDataSet().where_not_null(["col_string"])),
        (
            "col_string IS NOT NULL AND col_int >= 3",
            SampleDataSet().where_int_greater_equal_3().where_not_null(["col_string"]),
        ),
    ],
)
@pytest.mark.parametrize("df_type", [pd.DataFrame, dict])
def test_read_with_row_filter(df_type, backend: PostgresBackend, filter_expr, expected_data):
    """Is the full dataset correctly read?"""
    reset_test_table(True)
    if df_type == pd.DataFrame:
        df = backend.read_to_pandas("testdb", row_filter=filter_expr)
    elif df_type == dict:
        df = backend.read_to_dict("testdb", row_filter=filter_expr)
    expected_data.assert_correct_and_equal(df)


def test_read_to_pandas_top_3(backend: PostgresBackend):
    """Is the full dataset correctly read?"""
    reset_test_table(True)
    df = backend.read_to_pandas("testdb", limit=3)
    SampleDataSet().first_rows(3).assert_correct_and_equal(df)


def test_read_to_dict_top_3(backend: PostgresBackend):
    """Is the full dataset correctly read?"""
    reset_test_table(True)
    df = backend.read_to_dict("testdb", limit=3)
    SampleDataSet().first_rows(3).assert_correct_and_equal(df)


def test_read_to_pandas_sample_4(backend: PostgresBackend):
    """Is the full dataset correctly read?"""
    reset_test_table(True)
    df = backend.read_to_pandas("testdb", sample=4)
    SampleDataSchema.validate(df)
    assert len(df) == 4


def test_read_to_dict_sample_4(backend: PostgresBackend):
    """Is the full dataset correctly read?"""
    reset_test_table(True)
    df = backend.read_to_dict("testdb", sample=4)
    df = pd.DataFrame.from_records(df)
    SampleDataSchema.validate(df)
    assert len(df) == 4


def test_read_to_pandas_drop_duplicates(backend: PostgresBackend):
    """Is the full dataset correctly read?"""
    reset_test_table(True)
    df = backend.read_to_pandas("testdb", drop_duplicates=True)
    sample_data = SampleDataSet()
    sample_data.first_rows(len(sample_data) - 1).assert_correct_and_equal(df)


def test_read_to_dict_drop_duplicates(backend: PostgresBackend):
    """Is the full dataset correctly read?"""
    reset_test_table(True)
    df = backend.read_to_dict("testdb", drop_duplicates=True)
    sample_data = SampleDataSet()
    sample_data.first_rows(len(sample_data) - 1).assert_correct_and_equal(df)
