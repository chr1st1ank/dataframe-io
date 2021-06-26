# dataframe-io

[<img src="https://img.shields.io/pypi/v/dframeio.svg" alt="Release Status">](https://pypi.python.org/pypi/dframeio)
[<img src="https://github.com/chr1st1ank/dataframe-io/actions/workflows/test.yml/badge.svg?branch=main" alt="CI Status">](https://github.com/chr1st1ank/dataframe-io/actions)
[![codecov](https://codecov.io/gh/chr1st1ank/dataframe-io/branch/main/graph/badge.svg?token=4oBkRHXbfa)](https://codecov.io/gh/chr1st1ank/dataframe-io)


Read and write dataframes from and to any storage.

* Documentation: <https://chr1st1ank.github.io/dataframe-io/>
* License: Apache-2.0
* Status: Initial development

## Features

Supported input/output formats for the dataframes:
* pandas DataFrame
* Python dictionary

Supported storage backends:
* Parquet files

## Installation
```
pip install dframeio

# Including pyarrow to read/write parquet files:
pip install dframeio[pyarrow]
```

Show installed backends:
```
>>> import dframeio
>>> dframeio.backends
[<class 'dframeio.parquet.ParquetBackend'>]
```
