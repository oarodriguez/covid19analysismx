import os
from pathlib import Path
from typing import Dict

import pytest

from covid19analysismx import Config

TEST_ENV_VARS = {
    "DATA_DIR": "~/a-fake-dir",
    "DATABASE": "~/local-database.sqlite",
    "COVID_DATA_URL": "https://docs.python.org/3/library/dataclasses.html",
}


@pytest.fixture()
def test_env_vars():
    """Get the transformed environment variables."""
    test_data_dir = Path(TEST_ENV_VARS["DATA_DIR"]).expanduser().resolve()
    test_database = Path(TEST_ENV_VARS["DATABASE"]).expanduser().resolve()
    test_catalogs_dir = test_data_dir / "catalogs"
    return {
        "DATA_DIR": test_data_dir,
        "DATABASE": test_database,
        "CATALOGS_DIR": test_catalogs_dir,
        "COVID_DATA_URL": TEST_ENV_VARS["COVID_DATA_URL"],
    }


@pytest.fixture(autouse=True)
def setup_env_and_teardown():
    # Keep a copy of the original environment, and update it with the
    # given environment variables in TEST_ENV_VARS.
    old_environ = dict(os.environ)
    os.environ.update(TEST_ENV_VARS)
    # Execute tests...
    yield
    # Reset the original environment.
    os.environ.clear()
    os.environ.update(old_environ)


def test_from_environ(test_env_vars: Dict):
    """Test that the configuration from environment is built correctly."""
    conf = Config.from_environ()
    assert conf.DATA_DIR == test_env_vars["DATA_DIR"]
    assert conf.DATABASE == test_env_vars["DATABASE"]
    assert conf.CATALOGS_DIR == test_env_vars["CATALOGS_DIR"]
    assert conf.COVID_DATA_URL == test_env_vars["COVID_DATA_URL"]
