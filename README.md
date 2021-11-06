# dataframe-io

[<img src="https://img.shields.io/pypi/v/dframeio.svg" alt="Release Status">](https://pypi.python.org/pypi/dframeio)
[<img src="https://github.com/chr1st1ank/dataframe-io/actions/workflows/test.yml/badge.svg?branch=main" alt="CI Status">](https://github.com/chr1st1ank/dataframe-io/actions)
[![codecov](https://codecov.io/gh/chr1st1ank/dataframe-io/branch/main/graph/badge.svg?token=4oBkRHXbfa)](https://codecov.io/gh/chr1st1ank/dataframe-io)

Read and write dataframes from and to any storage.

* Documentation: <https://chr1st1ank.github.io/dataframe-io/>
* License: Apache-2.0
* Status: Initial development

## Features

Dataframes types supported:

* pandas DataFrame
* Python dictionary

Supported storage backends:

* Parquet files
* PostgreSQL database

More backends will come. Open an [issue](https://github.com/chr1st1ank/dataframe-io/issues)
if you are interested in a particular backend.

Implementation status for reading data:

| Storage       | Select columns | Filter rows | Max rows | Sampling | Drop duplicates |
| ------------- | :------------: | :---------: | :------: | :------: | :-------------: |
| Parquet files | ✔️              | ✔️           | ✔️        | ✔️        | ✔ ¹             |
| PostgreSQL    | ✔️              | ✔️           | ✔️        | ✔️        | ✔️               |

¹ only for pandas DataFrames

Implementation status for writing data:

| Storage       | write append | write replace |
| ------------- | :----------: | :-----------: |
| Parquet files | ✔️            | ✔️             |
| PostgreSQL    | ✔️            | ✔️             |

## Installation
```
pip install dframeio

# Including pyarrow to read/write parquet files:
pip install dframeio[parquet]

# Including PostgreSQL support:
pip install dframeio[postgres]
```

Show installed backends:
```
>>> import dframeio
>>> dframeio.backends
[<class 'dframeio.parquet.ParquetBackend'>]
```
