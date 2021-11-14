# History

## Unreleased
### Changed
- The mixed-type list of backends `dframeio.backends` was removed in favor of two lists `dframeio.read_backends` `dframeio.write_backends`. This is better suitable for the use case of backend discovery.

### Fixed
- PostgresBackend.read_to_dict() had an indentation error which made the function return a variable which was already out-of-scope.

## 0.3.0 (2021-11-08)
### Added
- Support for PostgreSQL
- Less strict version pinning for dependencies

### Fixed
- Package is installable without pyarrow now

## 0.2.0 (2021-06-19)

### Added
- Parquet file reading and writing with all features
- drameio.filter module for row filters in SQL syntax

## 0.1.1 (2021-06-04)

* Working draft of parquet file reading and writing

## 0.1.0 (2021-05-23)

* First release on PyPI.
