"""A project to analyze the evolution of the COVID-19 pandemic in Mexico."""

try:
    import importlib.metadata as importlib_metadata
except ModuleNotFoundError:
    import importlib_metadata  # type: ignore

from .config import Config

_metadata = importlib_metadata.metadata("covid19analysismx")  # type: ignore

# Export package information.
__version__ = _metadata["version"]

__all__ = [
    "Config",
    "__version__",
]
