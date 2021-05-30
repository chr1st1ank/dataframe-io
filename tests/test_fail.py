from dframeio import ParquetBackend


def test_provoke_some_failure():
    ParquetBackend(b"/some/funny/path", partitions=22)
