"""Abstract interfaces for all storage backends"""
from abc import abstractmethod
from typing import Any, Dict, List, Union

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
        columns: List[str],
        row_filter: str,
        drop_duplicates: bool = False,
        limit: int = -1,
        sample: int = -1,
    ) -> pd.DataFrame:
        """Read data into a pandas.DataFrame"""
        raise NotImplementedError()

    @abstractmethod
    def read_to_dict(
        self,
        source: str,
        columns: List[str],
        row_filter: str,
        drop_duplicates: bool = False,
        limit: int = -1,
        sample: int = -1,
    ) -> Dict[str, List]:
        """Read data into a dict of named columns"""
        raise NotImplementedError()


class AbstractDataFrameWriter:
    """Interface for writing dataframes to different storage drivers"""

    @abstractmethod
    def write_replace(
        self, target: str, dataframe: Union[pd.DataFrame, Dict[str, List[Any]]]
    ):
        """Write data with full replacement of an existing dataset"""
        raise NotImplementedError()

    @abstractmethod
    def write_append(
        self, target: str, dataframe: Union[pd.DataFrame, Dict[str, List[Any]]]
    ):
        """Write data in append-mode"""
        raise NotImplementedError()
