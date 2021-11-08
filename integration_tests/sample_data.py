"""Test dataset and schema for integration tests with multiple backends"""
from datetime import datetime, timedelta
from typing import Dict, List, Union

import pandas as pd
import pandera as pa
from pandas.testing import assert_frame_equal


class SampleDataSchema(pa.SchemaModel):
    """pandera schema of the test dataset"""

    col_timedelta: pa.typing.Series[pa.typing.Timedelta] = pa.Field(nullable=True)
    col_datetime: pa.typing.Series[pa.typing.DateTime] = pa.Field(nullable=True)
    col_bool: pa.typing.Series[pa.typing.Bool] = pa.Field(nullable=True)
    col_int: pa.typing.Series[pa.typing.Int] = pa.Field(nullable=True)
    col_string: pa.typing.Series[pa.typing.String] = pa.Field(nullable=True)
    col_float: pa.typing.Series[pa.typing.Float64] = pa.Field(nullable=True)


class SampleDataSet:
    """Test dataset with popular data types and some edge cases"""

    def __init__(self, data: pd.DataFrame = None):
        self._data: pd.DataFrame = (
            data
            if data is not None
            else pd.DataFrame(
                {
                    "col_timedelta": [
                        timedelta(seconds=s) if isinstance(s, int) else s
                        for s in [60, 50, 40, 30, 20, 10, timedelta(milliseconds=10000)]
                    ],
                    "col_datetime": pd.to_datetime(
                        [
                            datetime.fromisoformat(d) if isinstance(d, str) else d
                            for d in [
                                "2000-12-01T13:44:50",
                                "2000-12-01T13:44:50",
                                "2000-12-04T13:44:50",
                                "2000-12-05T13:44:50",
                                "2000-12-06T13:44:50",
                                "2000-12-07T13:44:50",
                                "2000-12-07T13:44:50",
                            ]
                        ]
                    ),
                    "col_bool": [True, False, False, False, False, False, False],
                    "col_int": [1, 2, 3, 4, 5, 16, 0x10],
                    "col_string": ["one", "two", "three", "four", "five", "six", "six"],
                    "col_float": [1.0, float("inf"), float("nan"), 4.1, 5.01, 0.02, 2e-2],
                },
            )
        )
        SampleDataSchema.to_schema().select_columns(self._data.columns).validate(self._data)

    def dataframe(self) -> pd.DataFrame:
        return self._data.copy()

    def datadict(self) -> Dict[str, List]:
        return self._data.to_dict(orient="list")

    def assert_correct_and_equal(self, other: Union[pd.DataFrame, dict]):
        """Check the schema of other and compare it to the sample dataset"""
        if isinstance(other, dict):
            other = pd.DataFrame.from_records(other)
        if not isinstance(other, pd.DataFrame):
            raise TypeError("other must be a dataframe or a dict!")
        # Sort cols
        cols = list(self._data.columns) + [c for c in other.columns if c not in self._data.columns]
        other = other[cols]
        SampleDataSchema.to_schema().select_columns(self._data.columns).validate(other)
        assert_frame_equal(
            self._data.sort_values(by=list(self._data.columns)).reset_index(drop=True),
            other.sort_values(by=list(self._data.columns)).reset_index(drop=True),
        )

    def __len__(self) -> int:
        """Number of rows in the data"""
        return len(self._data)

    def without_duplicates(self) -> "SampleDataSet":
        """Dataset with duplicates removed"""
        return SampleDataSet(self._data.drop_duplicates())

    def select_columns(self, columns: List[str]) -> "SampleDataSet":
        """Dataset reduced to the given columns"""
        return SampleDataSet(self._data[columns].copy())

    def first_rows(self, n: int) -> "SampleDataSet":
        """First n rows of the dataset"""
        return SampleDataSet(self._data.iloc[:n].copy())

    def where_int_greater_equal_3(self) -> "SampleDataSet":
        """Dataset with rows where col_int >= 3"""
        return SampleDataSet(self._data.query("col_int >= 3").reset_index(drop=True))

    def where_not_null(self, fields) -> "SampleDataSet":
        """Dataset with rows where not all fields are NULL"""
        return SampleDataSet(self._data.dropna(subset=fields))
