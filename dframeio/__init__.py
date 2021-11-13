"""Top-level package for dataframe-io."""
import typing

from .abstract import AbstractDataFrameReader, AbstractDataFrameWriter

from dframeio.abstract import AbstractDataFrameReader, AbstractDataFrameWriter

__version__ = "0.2.0"

# Add Backends one by one if dependencies are available
read_backends: typing.List[typing.Type[AbstractDataFrameReader]] = []
write_backends: typing.List[typing.Type[AbstractDataFrameWriter]] = []

try:
    from dframeio.parquet import ParquetBackend

    read_backends.append(ParquetBackend)
    write_backends.append(ParquetBackend)
except ModuleNotFoundError as e:
    if e.name == "pyarrow":
        pass
    else:
        raise

try:
    from dframeio.postgres import PostgresBackend

    read_backends.append(PostgresBackend)
    write_backends.append(PostgresBackend)
except ModuleNotFoundError as e:
    if e.name == "psycopg":
        pass
    else:
        raise
