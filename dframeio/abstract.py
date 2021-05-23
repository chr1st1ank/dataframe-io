"""Abstract interfaces for all storage backends"""
from abc import abstractmethod
from typing import Any, Dict, List, Union

from dframeio.imports import pd


class Reader:
    """Interface for reading dataframes from different storage drivers"""

    @abstractmethod()
    def read_to_pandas(
        self,
        source: str,
        columns: List[str],
        row_filter: str,
        drop_duplicates: bool = False,
        limit: int = -1,
        sample: int = -1,
    ):
        raise NotImplementedError()

    @abstractmethod()
    def read_to_dict(
        self,
        source: str,
        columns: List[str],
        row_filter: str,
        drop_duplicates: bool = False,
        limit: int = -1,
        sample: int = -1,
    ):
        raise NotImplementedError()


class Writer:
    """Interface for writing dataframes to different storage drivers"""

    @abstractmethod
    def write_replace(
        self, target: str, dataframe: Union[pd.Dataframe, Dict[str, List[Any]]]
    ):
        raise NotImplementedError()

    @abstractmethod
    def write_append(
        self, target: str, dataframe: Union[pd.Dataframe, Dict[str, List[Any]]]
    ):
        raise NotImplementedError()
