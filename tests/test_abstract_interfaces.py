"""Testing if all implementations match the interface of the abstract base classes"""
import inspect
import typing

import pytest

import dframeio
from dframeio import abstract


def method_names(cls: typing.Type) -> typing.List[str]:
    """Get a list of all method names of a Python class"""
    return [t[0] for t in inspect.getmembers(cls, predicate=inspect.isfunction)]


@pytest.mark.parametrize("function", method_names(abstract.AbstractDataFrameReader))
@pytest.mark.parametrize("backend", dframeio.backends)
def test_abstract_reader__methods_implemented(backend: typing.Type, function: str):
    """Checks if all functions of AbstractDataFrameReader are implemented """
    method = getattr(backend, function)
    assert not hasattr(
        method, "__isabstractmethod__"
    ), f"{function}() not implemented for {backend}"


@pytest.mark.parametrize("function", method_names(abstract.AbstractDataFrameReader))
@pytest.mark.parametrize("backend", dframeio.backends)
def test_abstract_reader_method_signatures(backend: typing.Type, function: str):
    """Checks function signatures of all AbstractDataFrameReader implementations

    This checks if the signature of all implemented methods are the same as in the
    base class. Arguments have to be the same, including names, default values and
    type hints.
    """
    abstract_signature = inspect.getfullargspec(getattr(abstract.AbstractDataFrameReader, function))
    concrete_signature = inspect.getfullargspec(getattr(backend, function))
    assert concrete_signature == abstract_signature
