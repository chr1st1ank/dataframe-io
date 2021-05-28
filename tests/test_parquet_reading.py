"""Unit tests for the reading functionality in dframeio.parquet"""
from pathlib import Path

import pandas as pd
import pandera as pa
import pandera.typing
import pytest

import dframeio

sample_data_nrows = 5000


class SampleDataSchema(pa.SchemaModel):
    registration_dttm: pa.typing.Series[pa.typing.DateTime]
    id: pa.typing.Series[pa.typing.Float64] = pa.Field(nullable=True)
    first_name: pa.typing.Series[pa.typing.String]
    last_name: pa.typing.Series[pa.typing.String]
    email: pa.typing.Series[pa.typing.String]
    gender: pa.typing.Series[pa.typing.String]
    ip_address: pa.typing.Series[pa.typing.String]
    cc: pa.typing.Series[pa.typing.String]
    country: pa.typing.Series[pa.typing.String]
    birthdate: pa.typing.Series[pa.typing.String]
    salary: pa.typing.Series[pa.typing.Float64] = pa.Field(nullable=True)
    title: pa.typing.Series[pa.typing.String]
    comments: pa.typing.Series[pa.typing.String] = pa.Field(nullable=True)
    @staticmethod
    def length():
        return 5000
    @staticmethod
    def n_salary_over_150000():
        return 2384


@pytest.fixture
def sample_data_path():
    return Path(__file__).parent / "data" / "parquet" / "multifile"


def test_read_to_pandas(sample_data_path):
    """Read a sample dataset into a pandas dataframe"""
    backend = dframeio.ParquetBackend(str(sample_data_path.parent))
    df = backend.read_to_pandas(sample_data_path.name)
    SampleDataSchema.to_schema().validate(df)
    assert len(df) == SampleDataSchema.length()


def test_read_to_pandas_some_columns(sample_data_path):
    """Read a sample dataset into a pandas dataframe, selecting some columns"""
    backend = dframeio.ParquetBackend(str(sample_data_path.parent))
    df = backend.read_to_pandas(sample_data_path.name, columns=["id", "first_name"])
    SampleDataSchema.to_schema().select_columns(["id", "first_name"]).validate(df)
    assert len(df) == SampleDataSchema.length()


def test_read_to_pandas_some_rows(sample_data_path):
    """Read a sample dataset into a pandas dataframe, filtering some rows"""
    backend = dframeio.ParquetBackend(str(sample_data_path.parent))
    df = backend.read_to_pandas(sample_data_path.name, row_filter="salary > 150000")
    SampleDataSchema.to_schema().validate(df)
    assert len(df) == SampleDataSchema.n_salary_over_150000()


def test_read_to_pandas_base_path_check(sample_data_path):
    """Try if it isn't possible to read from outside the base path"""
    backend = dframeio.ParquetBackend(str(sample_data_path.parent))
    with pytest.raises(ValueError):
        backend.read_to_pandas("/tmp")


def test_read_to_dict(sample_data_path):
    """Read a sample dataset into a dictionary"""
    backend = dframeio.ParquetBackend(str(sample_data_path.parent))
    df = backend.read_to_dict(sample_data_path.name)
    df = pd.DataFrame(df)
    SampleDataSchema.to_schema().validate(df)
    assert len(df) == SampleDataSchema.length()


def test_read_to_dict_some_columns(sample_data_path):
    """Read a sample dataset into a dictionary, filtering some columns"""
    backend = dframeio.ParquetBackend(str(sample_data_path.parent))
    df = backend.read_to_dict(sample_data_path.name, columns=["id", "first_name"])
    df = pd.DataFrame(df)
    SampleDataSchema.to_schema().select_columns(["id", "first_name"]).validate(df)
    assert len(df) == SampleDataSchema.length()


def test_read_to_dict_some_rows(sample_data_path):
    """Read a sample dataset into a dictionary, filtering some rows"""
    backend = dframeio.ParquetBackend(str(sample_data_path.parent))
    with pytest.raises(NotImplementedError):
        df = backend.read_to_dict(sample_data_path.name, row_filter="salary > 150000")
    # df = pd.DataFrame(df)
    # SampleDataSchema.to_schema().validate(df)
    # assert len(df) == SampleDataSchema.n_salary_over_150000()


def test_read_to_dict_base_path_check(sample_data_path):
    """Try if it isn't possible to read from outside the base path"""
    backend = dframeio.ParquetBackend(str(sample_data_path.parent))
    with pytest.raises(ValueError):
        backend.read_to_dict("/tmp")
