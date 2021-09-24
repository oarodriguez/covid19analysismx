"""Verify the library top-level functionality."""
import covid19mx


def test_version():
    """Verify we have updated the package version."""
    assert covid19mx.__version__ == "2021.2.0.dev0"
