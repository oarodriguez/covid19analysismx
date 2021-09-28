"""Define fixtures for tests that lie in the current directory."""

import os
from contextlib import contextmanager
from pathlib import Path

import pytest
import responses
from duckdb import DuckDBPyConnection, connect

from covid19mx import Config, COVIDData, DataManager

# Project directory path.
project_path = Path(__file__).parent.parent

# Environment variables for testing.
TEST_ENV_VARS = {
    "DATA_DIR": str(project_path / "__tests_data__"),
    "COVID_DATA_URL": (
        "http://datosabiertos.salud.gob.mx/gobmx/salud/datos_abiertos/"
        "historicos/2021/04/datos_abiertos_covid19_11.04.2021.zip"
    ),
}

# Testing data location.
LOCAL_DATA_FILE = (
    Path(__file__).parent
    / "data"
    / "datos_abiertos_covid19_11.04.2021.mock.zip"
)

# The following headers are set in true responses from the
# government website.
CONTENT_TYPE = "application/x-zip-compressed"
CONTENT_LENGTH = str(LOCAL_DATA_FILE.stat().st_size)
RESPONSE_HEADERS = {
    "Content-Length": CONTENT_LENGTH,
    "Accept-Ranges": "bytes",
}

# Location of the file containing the COVID data specification.
LOCAL_DATA_SPEC_FILE = (
    Path(__file__).parent / "data" / "diccionario_datos_covid19.zip"
)
LOCAL_DATA_SPEC_CONTENT_TYPE = "application/x-zip-compressed"
LOCAL_DATA_SPEC_CONTENT_LENGTH = str(LOCAL_DATA_SPEC_FILE.stat().st_size)
LOCAL_DATA_SPEC_RESPONSE_HEADERS = {
    "Content-Length": LOCAL_DATA_SPEC_CONTENT_LENGTH,
    "Accept-Ranges": "bytes",
}


@pytest.fixture(scope="module")
def config():
    """Init a configuration that points to an older COVID data version."""
    # Keep a copy of the original environment, and update it with the
    # given environment variables in TEST_ENV_VARS.
    old_environ = dict(os.environ)
    os.environ.update(TEST_ENV_VARS)
    # Create the environment and
    config = Config.from_environ()
    config.DATA_DIR.mkdir(parents=True, exist_ok=True)
    config.cache_dir.mkdir(parents=True, exist_ok=True)
    # Reset the original environment.
    os.environ.clear()
    os.environ.update(old_environ)
    # Execute tests...
    yield config


@pytest.fixture(scope="module")
def manager(config: Config):
    """Initialize a DataManager instance."""
    yield DataManager(config)


@contextmanager
def managed_covid_data(data: COVIDData):
    """Perform cleaning actions on COVID testing data."""
    assert data.path.exists()
    try:
        yield data
    finally:
        pass
        # FIXME: Keep extracted data by know, but eventually must be deleted.
        # data.path.unlink()
    # assert not data.path.exists()


@pytest.fixture(scope="module")
def covid_data(manager: DataManager):
    """Retrieve the testing data."""
    with LOCAL_DATA_FILE.open("rb") as fp:
        local_data_body = fp.read()
    # Create the mock response object and "download"
    # the COVID data accordingly.
    with responses.RequestsMock(
        assert_all_requests_are_fired=False
    ) as resp_mock:
        resp_mock.add(
            method=responses.GET,
            url=manager.config.COVID_DATA_URL,
            body=local_data_body,
            headers=RESPONSE_HEADERS,
            content_type=CONTENT_TYPE,
        )
        covid_data = manager.download_covid_data()
    with managed_covid_data(covid_data) as data:
        yield data


@pytest.fixture(scope="module")
def covid_data_spec(manager: DataManager):
    """Retrieve the testing data."""
    with LOCAL_DATA_SPEC_FILE.open("rb") as fp:
        local_data_body = fp.read()
    # Create the mock response object and "download"
    # the COVID data accordingly.
    with responses.RequestsMock(
        assert_all_requests_are_fired=False
    ) as resp_mock:
        resp_mock.add(
            method=responses.GET,
            url=manager.config.COVID_DATA_SPEC_URL,
            body=local_data_body,
            headers=LOCAL_DATA_SPEC_RESPONSE_HEADERS,
            content_type=LOCAL_DATA_SPEC_CONTENT_TYPE,
        )
        covid_data_spec = manager.download_covid_data_spec()
    yield covid_data_spec


@pytest.fixture(scope="module")
def covid_data_info(manager: DataManager):
    """Retrieve the testing data information."""
    # Create the mock response object and "download"
    # the COVID data info accordingly.
    with responses.RequestsMock(
        assert_all_requests_are_fired=False
    ) as resp_mock:
        resp_mock.add(
            method=responses.HEAD,
            url=manager.config.COVID_DATA_URL,
            headers=RESPONSE_HEADERS,
            content_type=CONTENT_TYPE,
        )
        data_info = manager.remote_covid_data_info()
    yield data_info


@pytest.fixture(scope="module")
def covid_data_spec_info(manager: DataManager):
    """Retrieve the testing data information."""
    # Create the mock response object and "download"
    # the COVID data info accordingly.
    with responses.RequestsMock(
        assert_all_requests_are_fired=False
    ) as resp_mock:
        resp_mock.add(
            method=responses.HEAD,
            url=manager.config.COVID_DATA_SPEC_URL,
            headers=LOCAL_DATA_SPEC_RESPONSE_HEADERS,
            content_type=LOCAL_DATA_SPEC_CONTENT_TYPE,
        )
        data_info = manager.remote_covid_data_spec_info()
    yield data_info


@pytest.fixture(scope="module")
def connection(config: Config) -> DuckDBPyConnection:
    """Yield an auto-closing SQLite connection to the system database."""
    yield connect(str(config.DATABASE))
