"""Top-level package for dataframe-io."""

__version__ = "0.2.0"

# Add Backends one by one if dependencies are available
backends = []

try:
    from dframeio.parquet import ParquetBackend

    backends.append(ParquetBackend)
except ModuleNotFoundError as e:
    if e.name == "pyarrow":
        pass
    else:
        raise

try:
    from dframeio.postgres import PostgresBackend

    backends.append(PostgresBackend)
except ModuleNotFoundError as e:
    if e.name == "psycopg":
        pass
    else:
        raise
