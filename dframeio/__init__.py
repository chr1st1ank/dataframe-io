"""Top-level package for dataframe-io."""

__version__ = "0.1.0"


# Add Backends one by one if dependencies are available
backends = []

try:
    import dframeio.parquet

    backends.append(dframeio.parquet.ParquetBackend)
except ModuleNotFoundError as e:
    if e.name == "pyarrow":
        pass
    else:
        raise
