"""Tests for dframeio.generic_connection"""
import pytest

import dframeio


@pytest.mark.parametrize("backend_type, backend_config", [
    (dframeio.ParquetBackend, dict(base_path=".")),
    ("ParquetBackend", dict(base_path=".")),
])
def test_init_parquet_reader(backend_type, backend_config):
    """Create a reader with the chosen backend"""
    backend = dframeio.create_reader(backend_type=backend_type, backend_config=backend_config)
    assert isinstance(backend, dframeio.ParquetBackend)


@pytest.mark.parametrize("backend_type, backend_config", [
    (dframeio.ParquetBackend, dict(base_path=".")),
    ("ParquetBackend", dict(base_path=".")),
])
def test_init_parquet_writer(backend_type, backend_config):
    """Create a writer with the chosen backend"""
    backend = dframeio.create_writer(backend_type=backend_type, backend_config=backend_config)
    assert isinstance(backend, dframeio.ParquetBackend)

