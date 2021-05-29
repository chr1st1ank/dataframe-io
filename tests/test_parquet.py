"""Unit tests for the reading functionality in dframeio.parquet"""
# pylint: disable=redefined-outer-name
from pathlib import Path

import pandas as pd
from pandas.util.testing import assert_frame_equal
import pandera as pa
import pytest

import dframeio


class SampleDataSchema(pa.SchemaModel):
    """pandera schema of the parquet test dataset"""

    registration_dttm: pa.typing.Series[pa.typing.DateTime]
    id: pa.typing.Series[pa.typing.Float64] = pa.Field(nullable=True)
    first_name: pa.typing.Series[pa.typing.String]
    last_name: pa.typing.Series[pa.typing.String]
    email: pa.typing.Series[pa.typing.String]
    gender: pa.typing.Series[pa.typing.String] = pa.Field(coerce=True)
    ip_address: pa.typing.Series[pa.typing.String]
    cc: pa.typing.Series[pa.typing.String]
    country: pa.typing.Series[pa.typing.String]
    birthdate: pa.typing.Series[pa.typing.String]
    salary: pa.typing.Series[pa.typing.Float64] = pa.Field(nullable=True)
    title: pa.typing.Series[pa.typing.String]
    comments: pa.typing.Series[pa.typing.String] = pa.Field(nullable=True)

    @staticmethod
    def length():
        """Known length of the data"""
        return 5000

    @staticmethod
    def n_salary_over_150000():
        """Number of rows with salary > 150000"""
        return 2384


@pytest.fixture(params=["multifile", "singlefile.parquet", "multifolder"])
def sample_data_path(request):
    """Path of a parquet dataset for testing"""
    return Path(__file__).parent / "data" / "parquet" / request.param


@pytest.fixture(scope="function")
def sample_dataframe():
    """Provide the sample dataframe"""
    parquet_file = Path(__file__).parent / "data" / "parquet" / "singlefile.parquet"
    backend = dframeio.ParquetBackend(str(parquet_file.parent))
    return backend.read_to_pandas(parquet_file.name)


@pytest.mark.parametrize(
    "kwargs, exception",
    [
        ({"base_path": "/some/dir", "partitions": -1}, TypeError),
        ({"base_path": "/some/dir", "partitions": 2.2}, TypeError),
        ({"base_path": "/some/dir", "partitions": "abc"}, TypeError),
        ({"base_path": "/some/dir", "partitions": b"abc"}, TypeError),
        ({"base_path": "/some/dir", "rows_per_file": b"abc"}, TypeError),
        ({"base_path": "/some/dir", "rows_per_file": 1.1}, TypeError),
        ({"base_path": "/some/dir", "rows_per_file": -5}, ValueError),
    ],
)
def test_init_argchecks(kwargs, exception):
    with pytest.raises(exception):
        dframeio.ParquetBackend(**kwargs)


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
        backend.read_to_dict(sample_data_path.name, row_filter="salary > 150000")


def test_read_to_dict_base_path_check(sample_data_path):
    """Try if it isn't possible to read from outside the base path"""
    backend = dframeio.ParquetBackend(str(sample_data_path.parent))
    with pytest.raises(ValueError):
        backend.read_to_dict("/tmp")


def test_write_replace_df(sample_dataframe, tmp_path_factory):
    """Write the dataframe, read it again and check identity"""
    tempdir = tmp_path_factory.mktemp("test_write_replace_df")

    backend = dframeio.ParquetBackend(str(tempdir))
    backend.write_replace("data.parquet", sample_dataframe)

    backend2 = dframeio.ParquetBackend(str(tempdir))
    dataframe_after = backend2.read_to_pandas("data.parquet")
    assert_frame_equal(dataframe_after, sample_dataframe)


def test_write_replace_multifile(sample_dataframe, tmp_path_factory):
    """Write the dataframe, read it again and check identity"""
    tempdir = tmp_path_factory.mktemp("test_write_replace_df")

    backend = dframeio.ParquetBackend(str(tempdir), rows_per_file=1000)
    backend.write_replace("data", sample_dataframe)

    assert sum(1 for _ in (tempdir / "data").glob("*")) == 5, "There should be 5 files"

    backend2 = dframeio.ParquetBackend(str(tempdir))
    dataframe_after = backend2.read_to_pandas("data")
    assert_frame_equal(dataframe_after, sample_dataframe)


def test_write_replace_partitioned(sample_dataframe, tmp_path_factory):
    """Write the dataframe, read it again and check identity"""
    tempdir = tmp_path_factory.mktemp("test_write_replace_df")

    backend = dframeio.ParquetBackend(str(tempdir), partitions=["gender"])
    backend.write_replace("data", sample_dataframe)

    created_partitions = {f.name for f in (tempdir / "data").glob("*=*")}
    assert created_partitions == {"gender=", "gender=Female", "gender=Male"}

    backend2 = dframeio.ParquetBackend(str(tempdir))
    dataframe_after = backend2.read_to_pandas("data")

    # It is o.k. to get the partition keys back as categoricals, because
    # that's more efficient. For comparison we make the column string again.
    dataframe_after = dataframe_after.assign(gender=dataframe_after["gender"].astype(str))
    assert_frame_equal(
        dataframe_after,
        sample_dataframe,
        check_like=True,
    )


@pytest.mark.parametrize("partitions", [[5], ["foobar"]])
def test_write_replace_invalid_partitions(tmp_path_factory, partitions):
    """Write the dataframe, read it again and check identity"""
    tempdir = tmp_path_factory.mktemp("test_write_replace_df")

    backend = dframeio.ParquetBackend(str(tempdir), partitions=partitions)
    with pytest.raises(ValueError):
        backend.write_replace("data.parquet", pd.DataFrame())
