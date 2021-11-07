"""Access parquet datasets using pyarrow.
"""
import collections.abc
import random
import re
import shutil
from pathlib import Path
from typing import Dict, Iterable, List, Union

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

import dframeio.filter

from .abstract import AbstractDataFrameReader, AbstractDataFrameWriter


class ParquetBackend(AbstractDataFrameReader, AbstractDataFrameWriter):
    """Backend to read and write parquet datasets

    Args:
        base_path: Base path for the parquet files. Only files in this folder or
            subfolders can be read from or written to.
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
            row_filter: Filter expression for selecting rows
            limit: Maximum number of rows to return (limit to first n rows)
            sample: Size of a random sample to return
            drop_duplicates: Whether to drop duplicate rows from the final selection

        Returns:
            A pandas DataFrame with the requested data.

        Raises:
            ValueError: If path specified with `source` is outside of the base path

        The logic of the filtering arguments is as documented for
        [`AbstractDataFrameReader.read_to_pandas()`](dframeio.abstract.AbstractDataFrameReader.read_to_pandas).
        """
        full_path = self._validated_full_path(source)
        df = self._read_parquet_table(
            full_path, columns=columns, row_filter=row_filter, limit=limit, sample=sample
        )
        if drop_duplicates:
            return df.to_pandas().drop_duplicates()
        return df.to_pandas()

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
            row_filter: Filter expression for selecting rows
            limit: Maximum number of rows to return (limit to first n rows)
            sample: Size of a random sample to return
            drop_duplicates: (Not supported!) Whether to drop duplicate rows

        Returns:
            A dictionary with column names as key and a list with column values as values

        Raises:
            NotImplementedError: When drop_duplicates is specified

        The logic of the filtering arguments is as documented for
        [`AbstractDataFrameReader.read_to_pandas()`](dframeio.abstract.AbstractDataFrameReader.read_to_pandas).
        """
        if drop_duplicates:
            raise NotImplementedError("drop_duplicates not available for Parquet -> dict")
        full_path = self._validated_full_path(source)
        df = self._read_parquet_table(
            full_path, columns=columns, row_filter=row_filter, limit=limit, sample=sample
        )
        return df.to_pydict()

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
            TypeError: When the dataframe is neither an pandas.DataFrame nor a dictionary
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
                    arrow_table, where=str(full_path), flavor="spark", compression="snappy"
                )

    def write_append(self, target: str, dataframe: Union[pd.DataFrame, Dict[str, List]]):
        """Write data in append-mode"""
        full_path = self._validated_full_path(target)
        if full_path.exists() and full_path.is_file():
            if isinstance(dataframe, pd.DataFrame):
                dataframe = pd.concat([self.read_to_pandas(str(full_path)), dataframe])
            elif isinstance(dataframe, collections.abc.Mapping):
                old_data = self._read_parquet_table(
                    str(full_path), use_pandas_metadata=False
                ).to_pydict()
                self._remove_matching_keys(old_data, r"__index_level_\d+__")
                if set(old_data.keys()) != set(dataframe.keys()):
                    raise ValueError(
                        "Can only append with identical columns. "
                        f"Existing columns: {set(old_data.keys())} "
                        f"New columns: {set(dataframe.keys())}."
                    )
                dataframe = {k: old_data[k] + dataframe[k] for k in old_data.keys()}
            else:
                raise TypeError(
                    "dataframe must be either a pandas.DataFrame or a dictionary. "
                    f"Got type {type(dataframe)}"
                )
            full_path.unlink()
        if self._rows_per_file > 0:
            full_path.mkdir(exist_ok=True)
            filename_index = 0
            for i in range(0, self._n_rows(dataframe), self._rows_per_file):
                while (full_path / (full_path.name + str(filename_index))).exists():
                    filename_index += 1
                pq.write_table(
                    self._dataframe_slice_as_arrow_table(dataframe, i, i + self._rows_per_file),
                    where=str(full_path / (full_path.name + str(filename_index))),
                    flavor="spark",
                    compression="snappy",
                )
                filename_index += 1
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

    @staticmethod
    def _remove_matching_keys(d: collections.abc.Mapping, regex: str):
        """Remove all keys matching regex from the dictionary d"""
        compiled_regex = re.compile(regex)
        keys_to_delete = [k for k in d.keys() if compiled_regex.match(k)]
        for k in keys_to_delete:
            del d[k]

    @staticmethod
    def _n_rows(dataframe: Union[pd.DataFrame, Dict[str, List]]) -> int:
        """Returns the number of rows in the dataframe

        Returns:
            Number of rows as int

        Raises:
            TypeError: When the dataframe is neither an pandas.DataFrame nor a dictionary
        """
        if isinstance(dataframe, pd.DataFrame):
            return len(dataframe)
        if isinstance(dataframe, collections.abc.Mapping):
            return len(next(iter(dataframe.values())))
        raise TypeError("dataframe must be a pandas.DataFrame or dict")

    @staticmethod
    def _dataframe_slice_as_arrow_table(
        dataframe: Union[pd.DataFrame, Dict[str, List]], start: int, stop: int
    ):
        if isinstance(dataframe, pd.DataFrame):
            return pa.Table.from_pandas(dataframe.iloc[start:stop], preserve_index=True)
        if isinstance(dataframe, collections.abc.Mapping):
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
        if isinstance(dataframe, collections.abc.Mapping):
            return pa.Table.from_pydict(dataframe)
        raise ValueError("dataframe must be a pandas.DataFrame or dict")

    @staticmethod
    def _read_parquet_table(
        full_path: Union[str, Path],
        columns: List[str] = None,
        row_filter: str = None,
        limit: int = -1,
        sample: int = -1,
        use_pandas_metadata: bool = True,
    ) -> pa.Table:
        """Read a parquet dataset from disk into a parquet.Table object

        Args:
            source: The full path of the file or folder with a parquet dataset to read
            columns: List of column names to limit the reading to
            row_filter: Filter expression for selecting rows
            limit: Maximum number of rows to return (limit to first n rows)
            use_pandas_metadata: Whether to read also pandas data such as index columns

        Returns:
            Content of the file as a pyarrow Table
        """
        kwargs = dict(columns=columns, use_threads=True, use_pandas_metadata=use_pandas_metadata)
        if row_filter:
            kwargs["filters"] = dframeio.filter.to_pyarrow_dnf(row_filter)
        df = pq.read_table(str(full_path), **kwargs)
        if limit >= 0:
            df = df.slice(0, min(df.num_rows, limit))
        if sample >= 0:
            indices = random.sample(range(df.num_rows), min(df.num_rows, sample))
            df = df.take(indices)
        return df
