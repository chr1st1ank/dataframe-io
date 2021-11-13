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
        A object of a backend class implementing the AbstractDataFrameReader interface.
    """
    pass


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
        A object of a backend class implementing the AbstractDataFrameWriter interface.
    """
    pass
