"""Test dataset and schema for integration tests with multiple backends"""
from datetime import datetime, timedelta
from typing import List

import pandas as pd
import pandera as pa


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
        self._data = data or pd.DataFrame(
            {
                "timedelta": [
                    timedelta(seconds=s) if isinstance(s, int) else s
                    for s in [50, None, 40, 30, 20, 10, timedelta(milliseconds=10000)]
                ],
                "datetime": [
                    datetime.fromisoformat(d) if isinstance(d, str) else d
                    for d in [
                        "2000-12-01T13:44:50Z",
                        None,
                        "2000-12-01T13:44:50+01:00",
                        "2000-12-04T13:44:50",
                        "2000-12-05T13:44:50",
                        "2000-12-06T13:44:50",
                        "2000-12-07T13:44:50",
                        "2000-12-07T13:44:50",
                    ]
                ],
                "bool": [True, None, False, False, False, False, False, False],
                "int": [1, None, 2, 3, 4, 5, 16, 0x10],
                "string": ["one", None, "two", "three", "four", "five", "six", "six"],
                "float": [1.0, None, float("inf"), float("nan"), 4.1, 5.01, 0.02, 2e-2],
            }
        )
        assert SampleDataSchema.to_schema().validate(self._data)

    def length(self):
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
        return SampleDataSet(self._data.query("int >= 3"))

    def where_not_null(self) -> "SampleDataSet":
        """Dataset with rows where not all fields are NULL"""
        return SampleDataSet(self._data.dropna())
