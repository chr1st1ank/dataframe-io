"""Access PostgreSQL databases using psycopg3.
"""
from typing import Dict, List, Union

import pandas as pd
import psycopg
import psycopg.sql as psql

from .abstract import AbstractDataFrameReader, AbstractDataFrameWriter
from .filter import to_psql


class PostgresBackend(AbstractDataFrameReader, AbstractDataFrameWriter):
    """Backend to read and write PostgreSQL tables

    Args:
        conninfo: Connection string in libq format. See the [PostgreSQL docs][1]
            for details.
        connection_factory: Alternative way of connecting is to provide a function returning
            an already established connection.

    Raises:
         ValueError: If any of the input arguments are outside of the documented
            value ranges or if conflicting arguments are given.
         TypeError: If any of the input arguments has a diffent type as documented

    [1]: https://www.postgresql.org/docs/current/libpq-connect.html#LIBPQ-CONNSTRING
    """

    _connection: psycopg.Connection
    _batch_size: int = 1000

    def __init__(self, conninfo: str = None, *, autocommit=True):
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
        [`read_to_pandas()`](dframeio.abstract.AbstractDataFrameReader.read_to_pandas).
        """
        query = self._make_psql_query(source, columns, row_filter, limit, sample, drop_duplicates)

        dataframe = pd.read_sql_query(query, self._connection)
        if drop_duplicates:
            return dataframe.drop_duplicates()
        return dataframe

    def read_to_dict(
        self,
        source: str,
        columns: List[str] = None,
        row_filter: str = None,
        limit: int = -1,
        sample: int = -1,
        drop_duplicates: bool = False,
    ) -> Dict[str, List]:
        """Read data into a dict of named columns

        Args:
            source: A string specifying the data source (format differs by backend)
            columns: List of column names to limit the reading to
            row_filter: NOT IMPLEMENTED. Reserved keyword for filtering rows.
            limit: Maximum number of rows to return (top-n)
            sample: Size of a random sample to return
            drop_duplicates: Whether to drop duplicate rows

        Returns:
            A dictionary with column names as key and a list with column values as values

        The logic of the filtering arguments is as documented for
        [`read_to_pandas()`](dframeio.abstract.AbstractDataFrameReader.read_to_pandas).
        """
        query = self._make_psql_query(source, columns, row_filter, limit, sample, drop_duplicates)

        with self._connection.cursor() as cursor:
            cursor.execute(query)
            # Preallocate dataframe as list of lists
            table = [[None] * cursor.rowcount for _ in range(cursor.pgresult.nfields)]
            # Fetch the data
            row_idx = 0
            while row_idx < cursor.rowcount:
                for row_as_tuples in cursor.fetchmany(size=self._batch_size):
                    # if row_as_tuples is not None:
                    for col_idx, cell in enumerate(row_as_tuples):
                        table[col_idx][row_idx] = cell
                    row_idx += 1

        return {column.name: table[i] for i, column in enumerate(cursor.description)}

    def _make_psql_query(
        self,
        source: str,
        columns: List[str] = None,
        row_filter: str = None,
        limit: int = -1,
        sample: int = -1,
        drop_duplicates: bool = False,
    ) -> psql.SQL:
        """Compose a full SQL query from the information given in the arguments.

        Args:
            source: The table name (may include a database name)
            columns: List of column names to limit the reading to
            row_filter: Filter expression for selecting rows
            limit: Maximum number of rows to return (limit to first n rows)
            sample: Size of a random sample to return

        Returns:
            Prepared query for psycopg
        """
        if limit != -1 and sample != -1:
            sample = min(limit, sample)
            limit = -1
        table = psql.Identifier(source)
        select = psql.SQL("SELECT DISTINCT") if drop_duplicates else psql.SQL("SELECT")
        columns_clause = self._make_columns_clause(columns)
        where_clause = self._make_where_clause(row_filter)
        limit_clause = self._make_limit_clause(limit)
        sample_clause = self._make_sample_clause(sample)
        query = psql.SQL(" ").join(
            [
                x
                for x in [
                    select,
                    columns_clause,
                    psql.SQL("FROM"),
                    table,
                    where_clause,
                    sample_clause,
                    limit_clause,
                ]
                if x
            ]
        )
        return query

    @staticmethod
    def _make_where_clause(row_filter: str = None) -> psql.SQL:
        """Create a psql WHERE clause from dframeio's standard SQL syntax"""
        if row_filter:
            psql_filter = to_psql(row_filter)
            return psql.SQL(f" WHERE {psql_filter}")
        return psql.SQL("")

    @staticmethod
    def _make_columns_clause(columns: List[str]) -> psql.SQL:
        """Create the list of columns for a select statement in psql syntax"""
        if isinstance(columns, str):
            raise TypeError(f"'columns' must be a list of column names. Got '{columns}'")
        return (
            psql.SQL(",").join([psql.Identifier(c) for c in columns]) if columns else psql.SQL("*")
        )

    @staticmethod
    def _make_sample_clause(sample: int) -> psql.Composed:
        """Create a sample clause in psql syntax if limit != -1, otherwise return empty clause"""
        if sample != -1:
            if not isinstance(sample, int):
                raise TypeError(f"'limit' must be a positive integer. Got {sample}")
            return psql.SQL(" ORDER BY RANDOM() LIMIT {sample}").format(sample=sample)
        return psql.Composed([])

    @staticmethod
    def _make_limit_clause(limit: int) -> psql.Composed:
        """Create a limit clause in psql syntax if limit != -1, otherwise return empty clause"""
        if limit != -1:
            if not isinstance(limit, int):
                raise TypeError(f"'limit' must be a positive integer. Got {limit}")
            return psql.SQL(" LIMIT {limit}").format(limit=limit)
        return psql.Composed([])

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
                cursor.executemany(query, zip(*dataframe.values()))
        else:
            raise TypeError("dataframe must either be a pandas DataFrame or a dict of lists")
