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


# Import pandas centrally here if installed, so that dframeio is also usable without
try:
    import pandas as pd
except ImportError:

    class PandasInterface:
        """Dummy for the uninstalled pandas package to allow using some names of it"""
        # pylint: disable=R0903  # too few public methods
        DataFrame = object
        Series = object

    pd = PandasInterface
