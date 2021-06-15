"""Unit tests for the reading functionality in dframeio.parquet"""
# pylint: disable=redefined-outer-name
from pathlib import Path

import pandas as pd
import pandera as pa
import pytest
from pandas.util.testing import assert_frame_equal

import dframeio


class SampleDataSchema(pa.SchemaModel):
    """pandera schema of the parquet test dataset"""

    registration_dttm: pa.typing.Series[pa.typing.DateTime]
    id: pa.typing.Series[pa.typing.Int] = pa.Field(nullable=True)
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


def read_sample_dataframe():
    """Read the sample dataframe to pandas and return a cached copy"""
    if not hasattr(read_sample_dataframe, "df"):
        parquet_file = Path(__file__).parent / "data" / "parquet" / "singlefile.parquet"
        backend = dframeio.ParquetBackend(str(parquet_file.parent))
        read_sample_dataframe.df = backend.read_to_pandas(parquet_file.name)
    return read_sample_dataframe.df.copy()


@pytest.fixture(scope="function")
def sample_dataframe():
    """Provide the sample dataframe"""
    return read_sample_dataframe()


@pytest.fixture(scope="function")
def sample_dataframe_dict():
    """Provide the sample dataframe"""
    parquet_file = Path(__file__).parent / "data" / "parquet" / "singlefile.parquet"
    backend = dframeio.ParquetBackend(str(parquet_file.parent))
    return backend.read_to_dict(parquet_file.name)


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
    """Challenge the argument validation of the constructor"""
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


def test_read_to_pandas_sample(sample_data_path):
    """Read a sample dataset into a pandas dataframe, filtering some rows"""
    backend = dframeio.ParquetBackend(str(sample_data_path.parent))
    df = backend.read_to_pandas(sample_data_path.name, sample=10)
    SampleDataSchema.to_schema().validate(df)
    assert len(df) == 10


@pytest.mark.parametrize("limit", [0, 10])
def test_read_to_pandas_limit(sample_data_path, limit):
    """Read a sample dataset into a pandas dataframe, filtering some rows"""
    backend = dframeio.ParquetBackend(str(sample_data_path.parent))
    df = backend.read_to_pandas(sample_data_path.name, limit=limit)
    SampleDataSchema.to_schema().validate(df)
    assert len(df) == limit


def test_read_to_pandas_base_path_check(sample_data_path):
    """Try if it isn't possible to read from outside the base path"""
    backend = dframeio.ParquetBackend(str(sample_data_path.parent))
    with pytest.raises(ValueError):
        backend.read_to_pandas("/tmp")


def test_read_to_dict(sample_data_path):
    """Read a sample dataset into a dictionary"""
    backend = dframeio.ParquetBackend(str(sample_data_path.parent))
    df = backend.read_to_dict(sample_data_path.name)
    assert isinstance(df, dict)
    assert set(df.keys()) == SampleDataSchema.to_schema().columns.keys()
    df = pd.DataFrame(df)
    SampleDataSchema.to_schema().validate(df)
    assert len(df) == SampleDataSchema.length()


def test_read_to_dict_some_columns(sample_data_path):
    """Read a sample dataset into a dictionary, filtering some columns"""
    backend = dframeio.ParquetBackend(str(sample_data_path.parent))
    df = backend.read_to_dict(sample_data_path.name, columns=["id", "first_name"])
    assert isinstance(df, dict)
    assert set(df.keys()) == {"id", "first_name"}
    df = pd.DataFrame(df)
    SampleDataSchema.to_schema().select_columns(["id", "first_name"]).validate(df)
    assert len(df) == SampleDataSchema.length()


def test_read_to_dict_some_rows(sample_data_path):
    """Read a sample dataset into a dictionary, filtering some rows"""
    backend = dframeio.ParquetBackend(str(sample_data_path.parent))
    df = backend.read_to_dict(sample_data_path.name, row_filter="salary > 150000")
    assert isinstance(df, dict)
    assert set(df.keys()) == SampleDataSchema.to_schema().columns.keys()
    df = pd.DataFrame(df)
    SampleDataSchema.to_schema().validate(df)
    assert len(df) == SampleDataSchema.n_salary_over_150000()


def test_read_to_dict_limit(sample_data_path):
    """Read a sample dataset into a dictionary, filtering some rows"""
    backend = dframeio.ParquetBackend(str(sample_data_path.parent))
    df = backend.read_to_dict(sample_data_path.name, columns=["id", "first_name"], limit=10)
    assert isinstance(df, dict)
    assert set(df.keys()) == {"id", "first_name"}
    df = pd.DataFrame(df)
    SampleDataSchema.to_schema().select_columns(["id", "first_name"]).validate(df)
    assert len(df) == 10


def test_read_to_dict_sample(sample_data_path):
    """Read a sample dataset into a dictionary, filtering some rows"""
    backend = dframeio.ParquetBackend(str(sample_data_path.parent))
    df = backend.read_to_dict(sample_data_path.name, sample=10)
    assert isinstance(df, dict)
    assert set(df.keys()) == SampleDataSchema.to_schema().columns.keys()
    df = pd.DataFrame(df)
    SampleDataSchema.to_schema().validate(df)
    assert len(df) == 10


def test_read_to_dict_base_path_check(sample_data_path):
    """Try if it isn't possible to read from outside the base path"""
    backend = dframeio.ParquetBackend(str(sample_data_path.parent))
    with pytest.raises(ValueError):
        backend.read_to_dict("/tmp")


@pytest.mark.parametrize("old_content", [False, True])
def test_write_replace_df(sample_dataframe, tmp_path_factory, old_content):
    """Write the dataframe, read it again and check identity"""
    tempdir = tmp_path_factory.mktemp("test_write_replace_df")
    if old_content:
        (tempdir / "data.parquet").open("w").close()

    backend = dframeio.ParquetBackend(str(tempdir))
    backend.write_replace("data.parquet", sample_dataframe)

    backend2 = dframeio.ParquetBackend(str(tempdir))
    dataframe_after = backend2.read_to_pandas("data.parquet")
    assert_frame_equal(dataframe_after, sample_dataframe)


@pytest.mark.parametrize("old_content", [False, True])
def test_write_replace_df_multifile(sample_dataframe, tmp_path_factory, old_content):
    """Write the dataframe, read it again and check identity"""
    tempdir = tmp_path_factory.mktemp("test_write_replace_df")
    if old_content:
        (tempdir / "data").mkdir()
        (tempdir / "data" / "old.parquet").open("w").close()

    backend = dframeio.ParquetBackend(str(tempdir), rows_per_file=1000)
    backend.write_replace("data", sample_dataframe)

    assert sum(1 for _ in (tempdir / "data").glob("*")) == 5, "There should be 5 files"

    if old_content:
        assert not (tempdir / "data" / "old.parquet").exists()
    backend2 = dframeio.ParquetBackend(str(tempdir))
    dataframe_after = backend2.read_to_pandas("data")
    assert_frame_equal(dataframe_after, sample_dataframe)


@pytest.mark.parametrize("old_content", [False, True])
def test_write_replace_df_partitioned(sample_dataframe, tmp_path_factory, old_content):
    """Write the dataframe, read it again and check identity"""
    tempdir = tmp_path_factory.mktemp("test_write_replace_df")
    if old_content:
        (tempdir / "data").mkdir()
        (tempdir / "data" / "old.parquet").open("w").close()

    backend = dframeio.ParquetBackend(str(tempdir), partitions=["gender"])
    backend.write_replace("data", sample_dataframe)

    created_partitions = {f.name for f in (tempdir / "data").glob("*=*")}
    assert created_partitions == {"gender=", "gender=Female", "gender=Male"}

    if old_content:
        assert not (tempdir / "data" / "old.parquet").exists()

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
def test_write_replace_df_invalid_partitions(tmp_path_factory, partitions):
    """Write the dataframe, read it again and check identity"""
    tempdir = tmp_path_factory.mktemp("test_write_replace_df")

    backend = dframeio.ParquetBackend(str(tempdir), partitions=partitions)
    with pytest.raises(ValueError):
        backend.write_replace("data.parquet", pd.DataFrame())


@pytest.mark.parametrize("old_content", [False, True])
def test_write_replace_dict(sample_dataframe_dict, tmp_path_factory, old_content):
    """Write the dataframe, read it again and check identity"""
    tempdir = tmp_path_factory.mktemp("test_write_replace_df")
    if old_content:
        (tempdir / "data.parquet").open("w").close()

    backend = dframeio.ParquetBackend(str(tempdir))
    backend.write_replace("data.parquet", sample_dataframe_dict)

    backend2 = dframeio.ParquetBackend(str(tempdir))
    dataframe_after = backend2.read_to_dict("data.parquet")
    assert dataframe_after == sample_dataframe_dict


@pytest.mark.parametrize("old_content", [False, True])
def test_write_replace_dict_multifile(sample_dataframe_dict, tmp_path_factory, old_content):
    """Write the dataframe, read it again and check identity"""
    tempdir = tmp_path_factory.mktemp("test_write_replace_df")
    if old_content:
        (tempdir / "data").mkdir()
        (tempdir / "data" / "old.parquet").open("w").close()

    backend = dframeio.ParquetBackend(str(tempdir), rows_per_file=1000)
    backend.write_replace("data", sample_dataframe_dict)

    assert sum(1 for _ in (tempdir / "data").glob("*")) == 5, "There should be 5 files"

    if old_content:
        assert not (tempdir / "data" / "old.parquet").exists()
    backend2 = dframeio.ParquetBackend(str(tempdir))
    dataframe_after = backend2.read_to_dict("data")
    assert dataframe_after == sample_dataframe_dict


@pytest.mark.parametrize("old_content", [False, True])
def test_write_replace_dict_partitioned(sample_dataframe_dict, tmp_path_factory, old_content):
    """Write the dataframe, read it again and check identity"""
    tempdir = tmp_path_factory.mktemp("test_write_replace_df")
    if old_content:
        (tempdir / "data").mkdir()
        (tempdir / "data" / "old.parquet").open("w").close()

    backend = dframeio.ParquetBackend(str(tempdir), partitions=["gender"])
    backend.write_replace("data", sample_dataframe_dict)

    created_partitions = {f.name for f in (tempdir / "data").glob("*=*")}
    assert created_partitions == {"gender=", "gender=Female", "gender=Male"}

    if old_content:
        assert not (tempdir / "data" / "old.parquet").exists()

    backend2 = dframeio.ParquetBackend(str(tempdir))
    dataframe_after = backend2.read_to_pandas("data")
    # It is o.k. to get the partition keys back as categoricals, because
    # that's more efficient. For comparison we make the column string again.
    dataframe_after = dataframe_after.assign(gender=dataframe_after["gender"].astype(str))
    cols = list(dataframe_after.columns)
    assert_frame_equal(
        dataframe_after.sort_values(by=cols).reset_index(drop=True),
        pd.DataFrame(sample_dataframe_dict).sort_values(by=cols).reset_index(drop=True),
        check_like=True,
    )


@pytest.mark.parametrize("partitions", [[5], ["foobar"]])
def test_write_replace_dict_invalid_partitions(tmp_path_factory, partitions):
    """Write the dataframe, read it again and check identity"""
    tempdir = tmp_path_factory.mktemp("test_write_replace_df")

    backend = dframeio.ParquetBackend(str(tempdir), partitions=partitions)
    with pytest.raises(ValueError):
        backend.write_replace("data.parquet", {})


@pytest.fixture(params=["pandas", "dict"])
def first_chunk(request):
    """First n lines of the sample dataframe"""
    if request.param == "pandas":
        return read_sample_dataframe().iloc[:100]
    return read_sample_dataframe().iloc[:100].to_dict("list")


@pytest.fixture(params=["pandas", "dict"])
def second_chunk(request):
    if request.param == "pandas":
        return read_sample_dataframe().iloc[100:]
    return read_sample_dataframe().iloc[100:].to_dict("list")


def test_write_append_df(sample_dataframe, first_chunk, second_chunk, tmp_path_factory):
    """Write the dataframe in two pieces, read it again and check identity"""
    tempdir = tmp_path_factory.mktemp("test_write_append_df")

    # Write first chunk
    backend = dframeio.ParquetBackend(str(tempdir))
    backend.write_append("data.parquet", first_chunk)

    # Write second chunk
    backend = dframeio.ParquetBackend(str(tempdir))
    backend.write_append("data.parquet", second_chunk)

    # Read and compare results
    backend = dframeio.ParquetBackend(str(tempdir))
    dataframe_after = backend.read_to_pandas("data.parquet")
    assert_frame_equal(dataframe_after, sample_dataframe)


def test_write_append_df_multifile(sample_dataframe, first_chunk, second_chunk, tmp_path_factory):
    """Write the dataframe, read it again and check identity"""
    tempdir = tmp_path_factory.mktemp("test_write_append_df")
    (tempdir / "data").mkdir()

    # Write first chunk
    backend = dframeio.ParquetBackend(str(tempdir))
    backend.write_append(tempdir / "data" / "first.parquet", first_chunk)

    # Write second chunk
    backend = dframeio.ParquetBackend(str(tempdir), rows_per_file=1000)
    backend.write_append("data", second_chunk)

    # Validate
    assert sum(1 for _ in (tempdir / "data").glob("*")) == 6, "There should be 6 files"
    assert (tempdir / "data" / "first.parquet").exists()
    backend2 = dframeio.ParquetBackend(str(tempdir))
    dataframe_after = backend2.read_to_pandas("data")
    if isinstance(first_chunk, pd.DataFrame) and isinstance(second_chunk, pd.DataFrame):
        assert_frame_equal(dataframe_after, sample_dataframe, check_like=True)
    SampleDataSchema.to_schema().validate(dataframe_after)
    assert len(dataframe_after) == SampleDataSchema.length()


def test_write_append_df_partitioned(sample_dataframe, first_chunk, second_chunk, tmp_path_factory):
    """Write the dataframe, read it again and check identity"""
    tempdir = tmp_path_factory.mktemp("test_write_append_df")
    (tempdir / "data").mkdir()

    # Write first chunk
    backend = dframeio.ParquetBackend(str(tempdir), partitions=["gender"])
    backend.write_append(tempdir / "data" / "first.parquet", first_chunk)

    # Write second chunk
    backend = dframeio.ParquetBackend(str(tempdir), partitions=["gender"])
    backend.write_append("data", second_chunk)

    # Validate
    created_partitions = {f.name for f in (tempdir / "data").glob("*=*")}
    assert created_partitions == {"gender=", "gender=Female", "gender=Male"}

    backend2 = dframeio.ParquetBackend(str(tempdir))
    dataframe_after = backend2.read_to_pandas("data")
    # It is o.k. to get the partition keys back as categoricals, because
    # that's more efficient. For comparison we make the column string again.
    dataframe_after = dataframe_after.assign(gender=dataframe_after["gender"].astype(str))
    if isinstance(first_chunk, pd.DataFrame) and isinstance(second_chunk, pd.DataFrame):
        assert_frame_equal(dataframe_after, sample_dataframe, check_like=True)
    SampleDataSchema.to_schema().validate(dataframe_after)
    assert len(dataframe_after) == SampleDataSchema.length()


@pytest.mark.parametrize("partitions", [[5], ["foobar"]])
def test_write_append_df_invalid_partitions(tmp_path_factory, partitions):
    """Write the dataframe, read it again and check identity"""
    tempdir = tmp_path_factory.mktemp("test_write_append_df")

    backend = dframeio.ParquetBackend(str(tempdir), partitions=partitions)
    with pytest.raises(ValueError):
        backend.write_append("data.parquet", pd.DataFrame())
