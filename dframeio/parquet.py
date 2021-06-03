"""Implementation to access parquet datasets using pyarrow."""
import collections
import shutil
from pathlib import Path
from typing import Dict, Iterable, List, Union

from .abstract import AbstractDataFrameReader, AbstractDataFrameWriter

try:
    import pandas as pd
except ImportError:
    pd = None

try:
    import pyarrow as pa
    import pyarrow.parquet as pq
except ModuleNotFoundError as e:
    if e.name == "pyarrow":
        pq = None
    else:
        raise


class ParquetBackend(AbstractDataFrameReader, AbstractDataFrameWriter):
    """Backend to read and write parquet datasets

    Args:
        base_path:
        partitions: (For writing only) Columns to use for partitioning.
            If given, the write functions split the data into a parquet dataset.
            Subfolders with the following naming schema are created when writing:
            `column_name=value`.

            Per default data is written as a single file.

            Cannot be combined with rows_per_file.

        rows_per_file: (For writing only) If a positive integer value is given
            this specifies the desired number of rows per file. The data is then
            written to multiple files.

            Per default data is written as a single file.

            Cannot be combined with partitions.

    Raises:
         ValueError: If any of the input arguments are outside of the documented
            value ranges or if conflicting arguments are given.
         TypeError: If any of the input arguments has a diffent type as documented
    """

    def __init__(self, base_path: str, partitions: Iterable[str] = None, rows_per_file: int = 0):
        self._base_path = base_path

        if partitions is not None and rows_per_file != 0:
            raise ValueError("Only one of 'partitions' and 'rows_per_file' can be used.")
        if rows_per_file != 0:
            if not isinstance(rows_per_file, int):
                raise TypeError(
                    f"Expected a positive integer for rows_per_file, but got {rows_per_file}."
                )
            if rows_per_file < 0:
                raise ValueError(
                    f"Expected a positive integer for rows_per_file, but got {rows_per_file}."
                )
        if partitions is not None:
            if isinstance(partitions, (str, bytes)):
                raise TypeError("partitions must be an integer or an iterable of column names")
            for _ in partitions:  # Raises TypeError if not iterable
                break
        self._partitions = partitions
        self._rows_per_file = rows_per_file

    def read_to_pandas(
        self,
        source: str,
        columns: List[str] = None,
        row_filter: str = None,
        limit: int = -1,
        sample: int = -1,
        drop_duplicates: bool = False,
    ) -> pd.DataFrame:
        """Read a parquet dataset from disk into a pandas DataFrame

        Args:
            source: The path of the file or folder with a parquet dataset to read
            columns: List of column names to limit the reading to
            row_filter: Filter expression for selecting rows.
            limit: Maximum number of rows to return (top-n)
            sample: Size of a random sample to return
            drop_duplicates: Whether to drop duplicate rows from the final selection

        Returns:
            A pandas DataFrame with the requested data.

        Raises:
            ValueError: If path specified with `source` is outside of the base path

        The logic of the filtering arguments is as documented for
        [`AbstractDataFrameReader.read_to_pandas()`](dframeio.abstract.AbstractDataFrameReader.read_to_pandas).
        """
        full_path = Path(self._base_path) / source
        if Path(self._base_path) not in full_path.parents:
            raise ValueError(
                f"The given source path {source} is not in base_path {self._base_path}!"
            )
        # TODO: use read_pandas()
        #   https://arrow.apache.org/docs/python/generated/pyarrow.parquet.read_pandas.html#pyarrow.parquet.read_pandas
        df = pq.read_table(
            str(full_path), columns=columns, use_threads=True, use_pandas_metadata=True
        ).to_pandas()
        if row_filter:
            df = df.query(row_filter)
        if limit > 0:
            df = df.head(limit)
        if sample > 0:
            df = df.sample(sample)
        return df

    def read_to_dict(
        self,
        source: str,
        columns: List[str] = None,
        row_filter: str = None,
        limit: int = -1,
        sample: int = -1,
        drop_duplicates: bool = False,
    ) -> Dict[str, List]:
        """Read a parquet dataset from disk into a dictionary of columns

        Args:
            source: The path of the file or folder with a parquet dataset to read
            columns: List of column names to limit the reading to
            row_filter: NOT IMPLEMENTED. Reserved keyword for filtering rows.
            limit: Maximum number of rows to return (top-n)
            sample: Size of a random sample to return
            drop_duplicates: Whether to drop duplicate rows

        Returns:
            A dictionary with column names as key and a list with column values as values

        Raises:
            NotImplementedError: If row_filter is given, because this is not yet implemented

        The logic of the filtering arguments is as documented for
        [`AbstractDataFrameReader.read_to_pandas()`](dframeio.abstract.AbstractDataFrameReader.read_to_pandas).
        """
        full_path = self._validated_full_path(source)
        df = pq.read_table(
            str(full_path), columns=columns, use_threads=True, use_pandas_metadata=True
        ).to_pydict()
        if row_filter:
            # TODO: Pyarrow supports filtering on loading
            #  https://arrow.apache.org/docs/python/generated/pyarrow.parquet.ParquetDataset.html
            raise NotImplementedError("Row filtering is not implemented for dicts")
        return df

    def write_replace(self, target: str, dataframe: Union[pd.DataFrame, Dict[str, List]]):
        """Write data with full replacement of an existing dataset

        Args:
            target: The path of the file or folder to write to. The path may be absolute
                or relative to the base_path given in the
                [`__init__()`](dframeio.parquet.ParquetBackend) function.
            dataframe: The data to write as pandas.DataFrame or as a Python dictionary
                in the format `column_name: [column_data]`

        Raises:
             ValueError: If the dataframe does not contain the columns to partition by
                as specified in the [`__init__()`](dframeio.parquet.ParquetBackend)
                function.
        """
        full_path = self._validated_full_path(target)
        if full_path.exists():
            if full_path.is_file():
                full_path.unlink()
            elif full_path.is_dir():
                shutil.rmtree(str(full_path), ignore_errors=True)
        if self._rows_per_file > 0:
            full_path.mkdir(exist_ok=True)
            for i in range(0, self._n_rows(dataframe), self._rows_per_file):
                pq.write_table(
                    self._dataframe_slice_as_arrow_table(dataframe, i, i + self._rows_per_file),
                    where=str(full_path / (full_path.name + str(i))),
                    flavor="spark",
                    compression="snappy",
                )
        else:
            arrow_table = self._to_arrow_table(dataframe)

            if self._partitions is not None:
                missing_columns = set(self._partitions) - set(arrow_table.column_names)
                if missing_columns:
                    raise ValueError(
                        f"Expected the dataframe to have the partition columns {missing_columns}"
                    )
                pq.write_to_dataset(
                    arrow_table,
                    root_path=str(full_path),
                    partition_cols=self._partitions,
                    flavor="spark",
                    compression="snappy",
                )
            else:
                pq.write_table(
                    arrow_table,
                    where=str(full_path),
                    flavor="spark",
                    compression="snappy",
                )

    def write_append(self, target: str, dataframe: Union[pd.DataFrame, Dict[str, List]]):
        """Write data in append-mode"""
        # TODO: Implement
        raise NotImplementedError()

    @staticmethod
    def _n_rows(dataframe):
        if isinstance(dataframe, pd.DataFrame):
            return len(dataframe)
        if isinstance(dataframe, collections.Mapping):
            return len(next(iter(dataframe.values())))
        raise ValueError("dataframe must be a pandas.DataFrame or dict")

    @staticmethod
    def _dataframe_slice_as_arrow_table(
        dataframe: Union[pd.DataFrame, Dict[str, List]], start: int, stop: int
    ):
        if isinstance(dataframe, pd.DataFrame):
            return pa.Table.from_pandas(dataframe.iloc[start:stop], preserve_index=True)
        if isinstance(dataframe, collections.Mapping):
            return pa.Table.from_pydict(
                {colname: col[start:stop] for colname, col in dataframe.items()}
            )
        raise ValueError("dataframe must be a pandas.DataFrame or dict")

    def _validated_full_path(self, path: Union[str, Path]) -> Path:
        """Make sure the given path is in self._base_path and return the full path

        Returns:
            The full path as pathlib object

        Raises:
            ValueError: If the path is not in the base path
        """
        full_path = Path(self._base_path) / path
        if Path(self._base_path) not in full_path.parents:
            raise ValueError(f"The given path {path} is not in base_path {self._base_path}!")
        return full_path

    @staticmethod
    def _to_arrow_table(dataframe: Union[pd.DataFrame, Dict[str, List]]):
        """Convert the dataframe to an arrow table"""
        if isinstance(dataframe, pd.DataFrame):
            return pa.Table.from_pandas(dataframe, preserve_index=True)
        if isinstance(dataframe, collections.Mapping):
            return pa.Table.from_pydict(dataframe)
        raise ValueError("dataframe must be a pandas.DataFrame or dict")
