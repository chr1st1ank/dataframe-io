import typing

import pytest

import dframeio
from dframeio import abstract


@pytest.mark.parametrize("function", ["read_to_pandas", "read_to_dict"])
@pytest.mark.parametrize("backend", dframeio.backends)
def test_reader_signature(backend, function):
    abstract_signature = typing.get_type_hints(getattr(abstract.Reader, function))
    concrete_signature = typing.get_type_hints(getattr(abstract.Reader, function))
    assert abstract_signature == concrete_signature
