"""Opening connections / initialize backends through a generic interface"""
from typing import Dict, Type, Union

import dframeio


def create_reader(
    backend_type: Union[Type[dframeio.AbstractDataFrameReader], str], backend_config: Dict
) -> dframeio.AbstractDataFrameReader:
    """Create a connection for reading using the specified backend

    Args:
        backend_type: Backend type to create. Either as type object or its name as a string.
        backend_config: The configuration for the backend as a dictionary of keyword arguments to
            initialize the backend with. See the `__init__()` functions of the different backends
            for the options.

    Returns:
        An object of a backend class implementing the AbstractDataFrameReader interface.
    """
    if isinstance(backend_type, str):
        for t in dframeio.read_backends:
            if t.__name__ == backend_type:
                return t(**backend_config)
    return backend_type(**backend_config)


def create_writer(
    backend_type: Union[Type[dframeio.AbstractDataFrameWriter], str], backend_config: Dict
) -> dframeio.AbstractDataFrameWriter:
    """Create a connection for writing using the specified backend

    Args:
        backend_type: Backend type to create. Either as type object or its name as a string.
        backend_config: The configuration for the backend as a dictionary of keyword arguments to
            initialize the backend with. See the `__init__()` functions of the different backends
            for the options.

    Returns:
        An object of a backend class implementing the AbstractDataFrameWriter interface.
    """
    if isinstance(backend_type, str):
        for t in dframeio.write_backends:
            if t.__name__ == backend_type:
                return t(**backend_config)
    return backend_type(**backend_config)
