"""Implementation to access parquet datasets using pyarrow."""
from pathlib import Path
from typing import List, Dict

from . import abstract

try:
    import pandas as pd
except ImportError:
    pd = None

try:
    import pyarrow.parquet as pq
except ModuleNotFoundError as e:
    if e.name == "pyarrow":
        pq = None
    else:
        raise


class ParquetBackend(abstract.AbstractDataFrameReader):
    """Backend to read and write parquet datasets"""
    def __init__(self, base_path: str):
        self.base_path = base_path

    def read_to_pandas(
        self,
        source: str,
        columns: List[str] = None,
        row_filter: str = None,
        drop_duplicates: bool = False,
        limit: int = -1,
        sample: int = -1,
    ) -> pd.DataFrame:
        """Read a parquet dataset from disk into a pandas dataframe"""
        full_path = Path(self.base_path) / source
        if Path(self.base_path) not in full_path.parents:
            raise ValueError(
                f"The given source path {source} is not in base_path {self.base_path}!"
            )
        df = pq.read_table(
            str(full_path), columns=columns, use_threads=True, use_pandas_metadata=True
        ).to_pandas()
        if row_filter:
            return df.query(row_filter)
        return df

    def read_to_dict(
        self,
        source: str,
        columns: List[str] = None,
        row_filter: str = None,
        drop_duplicates: bool = False,
        limit: int = -1,
        sample: int = -1,
    ) -> Dict[str, List]:
        """Read a parquet dataset from disk into a dictionary of columns"""
        full_path = Path(self.base_path) / source
        if Path(self.base_path) not in full_path.parents:
            raise ValueError(
                f"The given source path {source} is not in base_path {self.base_path}!"
            )
        df = pq.read_table(
            str(full_path), columns=columns, use_threads=True, use_pandas_metadata=True
        ).to_pydict()
        if row_filter:
            # TODO: Pyarrow supports filtering on loading
            #  https://arrow.apache.org/docs/python/generated/pyarrow.parquet.ParquetDataset.html
            raise NotImplementedError("Row filtering is not implemented for dicts")
        return df
