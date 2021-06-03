"""Abstract interfaces for all storage backends"""
from abc import abstractmethod
from typing import Dict, List, Union

try:
    import pandas as pd
except ImportError:
    pd = None


class AbstractDataFrameReader:
    """Interface for reading dataframes from different storage drivers"""

    @abstractmethod
    def read_to_pandas(
        self,
        source: str,
        columns: List[str] = None,
        row_filter: str = None,
        limit: int = -1,
        sample: int = -1,
        drop_duplicates: bool = False,
    ) -> pd.DataFrame:
        """Read data into a pandas.DataFrame

        Args:
            source: A string specifying the data source (format differs by backend)
            columns: List of column names to limit the reading to
            row_filter: Filter expression for selecting rows.
            limit: Maximum number of rows to return (top-n)
            sample: Size of a random sample to return
            drop_duplicates: Whether to drop duplicate rows from the final selection

        Returns:
            A pandas DataFrame with the requested data.

        The filter and limit arguments are applied in the following order:

        - first the `row_filter` expression is applied and all matching rows go into the next step,
        - afterwards the `limit` argument is applied if given,
        - in the next step the `sample` argument is applied if it is specified,
        - at the very end `drop_duplicates` takes effect. This means that this flag may reduce
          the output size further and that fewer rows may be returned as specified with `limit`
          or `sample` if there are duplicates in the data.
        """
        raise NotImplementedError()

    @abstractmethod
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
        raise NotImplementedError()


class AbstractDataFrameWriter:
    """Interface for writing dataframes to different storage drivers"""

    @abstractmethod
    def write_replace(self, target: str, dataframe: Union[pd.DataFrame, Dict[str, List]]):
        """Write data with full replacement of an existing dataset"""
        raise NotImplementedError()

    @abstractmethod
    def write_append(self, target: str, dataframe: Union[pd.DataFrame, Dict[str, List]]):
        """Write data in append-mode"""
        raise NotImplementedError()
