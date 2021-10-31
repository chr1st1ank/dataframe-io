"""Access parquet datasets using pyarrow.
"""
from typing import Dict, List, Union

import pandas as pd
import psycopg
import psycopg.sql as psql

from .abstract import AbstractDataFrameReader, AbstractDataFrameWriter


class PostgresBackend(AbstractDataFrameReader, AbstractDataFrameWriter):
    """Backend to read and write PostgreSQL tables

    Args:
        conninfo: Connection string in libq format. See
            https://www.postgresql.org/docs/current/libpq-connect.html#LIBPQ-CONNSTRING
        connection_factory: Alternative way of connecting is to provide a function returning
            an already established connection.

    Raises:
         ValueError: If any of the input arguments are outside of the documented
            value ranges or if conflicting arguments are given.
         TypeError: If any of the input arguments has a diffent type as documented
    """

    _connection: psycopg.Connection

    def __init__(self, conninfo: str = None, *, autocommit=False):
        super().__init__()
        self._connection = psycopg.connect(conninfo=conninfo, autocommit=autocommit)

    def read_to_pandas(
        self,
        source: str,
        columns: List[str] = None,
        row_filter: str = None,
        limit: int = -1,
        sample: int = -1,
        drop_duplicates: bool = False,
    ) -> pd.DataFrame:
        """Read a postgres table into a pandas DataFrame

        Args:
            source: The table name (may include a database name)
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
        if limit != -1 and sample != -1:
            raise ValueError()  # TODO
        columns_clause = ",".join(len(columns) * ["%s"]) if columns else "*"
        where_clause = f" WHERE {row_filter}" if row_filter else ""
        limit_clause = f" LIMIT {int(limit)}" if limit != -1 else ""
        sample_clause = (
            f" ORDER BY RANDOM() LIMIT {int(sample)}" if sample != -1 and not limit_clause else ""
        )
        query = (
            f"SELECT {columns_clause} FROM %s{where_clause}{sample_clause}{limit_clause};" % source
        )
        dataframe = pd.read_sql_query(query, self._connection)  # , params=[source])
        if drop_duplicates:
            return dataframe.drop_duplicates()
        return dataframe

    #
    # def read_to_dict(
    #     self,
    #     source: str,
    #     columns: List[str] = None,
    #     row_filter: str = None,
    #     limit: int = -1,
    #     sample: int = -1,
    #     drop_duplicates: bool = False,
    # ) -> Dict[str, List]:
    #     """Read a parquet dataset from disk into a dictionary of columns
    #
    #     Args:
    #         source: The path of the file or folder with a parquet dataset to read
    #         columns: List of column names to limit the reading to
    #         row_filter: Filter expression for selecting rows
    #         limit: Maximum number of rows to return (limit to first n rows)
    #         sample: Size of a random sample to return
    #         drop_duplicates: Whether to drop duplicate rows
    #
    #     Returns:
    #         A dictionary with column names as key and a list with column values as values
    #
    #     Raises:
    #         NotImplementedError: If row_filter is given, because this is not yet implemented
    #
    #     The logic of the filtering arguments is as documented for
    #     [`AbstractDataFrameReader.read_to_pandas()`](dframeio.abstract.AbstractDataFrameReader.read_to_pandas).
    #     """
    #     full_path = self._validated_full_path(source)
    #     df = self._read_parquet_table(
    #         full_path, columns=columns, row_filter=row_filter, limit=limit, sample=sample
    #     )
    #     return df.to_pydict()
    #
    def write_replace(self, target: str, dataframe: Union[pd.DataFrame, Dict[str, List]]):
        """Write data to a Postgres table after deleting all the existing content

        Args:
            target: The database table to write to.
            dataframe: The data to write as pandas.DataFrame or as a Python dictionary
                in the format `column_name: [column_data]`

        Raises:
            TypeError: When the dataframe is neither a pandas.DataFrame nor a dictionary
        """
        if not isinstance(dataframe, (pd.DataFrame, dict)):
            raise TypeError(
                "dataframe must either be a pandas DataFrame "
                f"or a dict of lists but was {dataframe}"
            )
        else:
            table = psql.Identifier(target)
            query = psql.SQL("DELETE FROM {table}").format(table=table)
            with self._connection.cursor() as cursor:
                cursor.execute(query)
            self.write_append(target, dataframe)

    def write_append(self, target: str, dataframe: Union[pd.DataFrame, Dict[str, List]]):
        """Write data in append-mode to a Postgres table

        Args:
            target: The database table to write to.
            dataframe: The data to write as pandas.DataFrame or as a Python dictionary
                in the format `column_name: [column_data]`

        Raises:
            TypeError: When the dataframe is neither a pandas.DataFrame nor a dictionary
        """
        table = psql.Identifier(target)
        if isinstance(dataframe, pd.DataFrame):
            columns = psql.SQL(",").join([psql.Identifier(c) for c in dataframe.columns])
            values = ", ".join(len(dataframe.columns) * ["%s"])
            query = psql.SQL("INSERT INTO {table}({columns}) VALUES (" + values + ")").format(
                table=table, columns=columns, values=values
            )
            with self._connection.cursor() as cursor:
                cursor.executemany(query, map(tuple, dataframe.where(dataframe.notnull()).values))
        elif isinstance(dataframe, dict):
            columns = psql.SQL(",").join([psql.Identifier(c) for c in dataframe])
            values = ", ".join(len(dataframe) * ["%s"])
            query = psql.SQL("INSERT INTO {table}({columns}) VALUES (" + values + ")").format(
                table=table, columns=columns, values=values
            )
            with self._connection.cursor() as cursor:
                # TODO: Work in batches here
                cursor.executemany(query, zip(*[v for v in dataframe.values()]))
        else:
            raise TypeError("dataframe must either be a pandas DataFrame or a dict of lists")

    # @staticmethod
    # def _remove_matching_keys(d: collections.Mapping, regex: str):
    #     """Remove all keys matching regex from the dictionary d"""
    #     compiled_regex = re.compile(regex)
    #     keys_to_delete = [k for k in d.keys() if compiled_regex.match(k)]
    #     for k in keys_to_delete:
    #         del d[k]
    #
    # @staticmethod
    # def _n_rows(dataframe: Union[pd.DataFrame, Dict[str, List]]) -> int:
    #     """Returns the number of rows in the dataframe
    #
    #     Returns:
    #         Number of rows as int
    #
    #     Raises:
    #         TypeError: When the dataframe is neither an pandas.DataFrame nor a dictionary
    #     """
    #     if isinstance(dataframe, pd.DataFrame):
    #         return len(dataframe)
    #     if isinstance(dataframe, collections.Mapping):
    #         return len(next(iter(dataframe.values())))
    #     raise TypeError("dataframe must be a pandas.DataFrame or dict")
    #
    # @staticmethod
    # def _dataframe_slice_as_arrow_table(
    #     dataframe: Union[pd.DataFrame, Dict[str, List]], start: int, stop: int
    # ):
    #     if isinstance(dataframe, pd.DataFrame):
    #         return pa.Table.from_pandas(dataframe.iloc[start:stop], preserve_index=True)
    #     if isinstance(dataframe, collections.Mapping):
    #         return pa.Table.from_pydict(
    #             {colname: col[start:stop] for colname, col in dataframe.items()}
    #         )
    #     raise ValueError("dataframe must be a pandas.DataFrame or dict")
    #
    # def _validated_full_path(self, path: Union[str, Path]) -> Path:
    #     """Make sure the given path is in self._base_path and return the full path
    #
    #     Returns:
    #         The full path as pathlib object
    #
    #     Raises:
    #         ValueError: If the path is not in the base path
    #     """
    #     full_path = Path(self._base_path) / path
    #     if Path(self._base_path) not in full_path.parents:
    #         raise ValueError(f"The given path {path} is not in base_path {self._base_path}!")
    #     return full_path
    #
    # @staticmethod
    # def _to_arrow_table(dataframe: Union[pd.DataFrame, Dict[str, List]]):
    #     """Convert the dataframe to an arrow table"""
    #     if isinstance(dataframe, pd.DataFrame):
    #         return pa.Table.from_pandas(dataframe, preserve_index=True)
    #     if isinstance(dataframe, collections.Mapping):
    #         return pa.Table.from_pydict(dataframe)
    #     raise ValueError("dataframe must be a pandas.DataFrame or dict")
    #
    # @staticmethod
    # def _read_parquet_table(
    #     full_path: str,
    #     columns: List[str] = None,
    #     row_filter: str = None,
    #     limit: int = -1,
    #     sample: int = -1,
    #     use_pandas_metadata: bool = True,
    # ) -> pa.Table:
    #     """Read a parquet dataset from disk into a parquet.Table object
    #
    #     Args:
    #         source: The full path of the file or folder with a parquet dataset to read
    #         columns: List of column names to limit the reading to
    #         row_filter: Filter expression for selecting rows
    #         limit: Maximum number of rows to return (limit to first n rows)
    #         use_pandas_metadata: Whether to read also pandas data such as index columns
    #
    #     Returns:
    #         Content of the file as a pyarrow Table
    #     """
    #     kwargs = dict(columns=columns, use_threads=True, use_pandas_metadata=use_pandas_metadata)
    #     if row_filter:
    #         kwargs["filters"] = dframeio.filter.to_pyarrow_dnf(row_filter)
    #     df = pq.read_table(str(full_path), **kwargs)
    #     if limit >= 0:
    #         df = df.slice(0, min(df.num_rows, limit))
    #     if sample >= 0:
    #         indices = random.sample(range(df.num_rows), min(df.num_rows, sample))
    #         df = df.take(indices)
    #     return df
