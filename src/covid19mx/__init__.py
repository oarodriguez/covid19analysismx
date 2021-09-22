"""A project to analyze the evolution of the COVID-19 pandemic in Mexico."""

try:
    import importlib.metadata as importlib_metadata
except ModuleNotFoundError:
    import importlib_metadata  # type: ignore

from .config import Config
from .data import (
    COVIDData,
    COVIDDataColumn,
    COVIDDataSpec,
    DataInfo,
    DataManager,
    DBDataManager,
)
from .log import console

metadata = importlib_metadata.metadata("covid19mx")  # type: ignore

# Export package information.
__version__ = metadata["version"]
__author__ = metadata["author"]
__description__ = metadata["description"]
__license__ = metadata["license"]

__all__ = [
    "COVIDData",
    "COVIDDataColumn",
    "COVIDDataSpec",
    "Config",
    "DBDataManager",
    "DataInfo",
    "DataManager",
    "__version__",
    "console",
    "metadata",
]
