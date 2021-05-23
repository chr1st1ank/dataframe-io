import pathlib
from typing import List

import pyarrow.parquet as pq

from dframeio import abstract


class ParquetBackend(abstract.Reader):
    def __init__(self, base_path: str):
        self.base_path = base_path

    def read_to_pandas(
        self,
        source: str,
        columns: List[str],
        row_filter: str,
        drop_duplicates: bool = False,
        limit: int = -1,
        sample: int = -1,
    ):
        """Read a parquet dataset from disk into a pandas dataframe"""
        full_path = pathlib.Path(self.base_path) / source
        if self.base_path not in full_path.parents:
            raise ValueError(
                f"The given source path {source} is not in base_path {self.base_path}!"
            )
        return pq.read_table(
            str(full_path), columns=columns, use_threads=True, use_pandas_metadata=True
        ).to_pandas()
