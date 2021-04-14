import os
from contextlib import closing, contextmanager
from pathlib import Path
from sqlite3 import Connection

import pytest
import responses

from covid19analysismx import Config, COVIDData, COVIDDataInfo, DataManager

# Project directory path.
project_path = Path(__file__).parent.parent

# Environment variables for testing.
TEST_ENV_VARS = {
    "DATA_DIR": str(project_path / "__tests_data__"),
    "CATALOGS_DIR": str(project_path / "data" / "catalogs"),
    "COVID_DATA_URL": (
        "http://datosabiertos.salud.gob.mx/gobmx/salud/datos_abiertos/"
        "historicos/04/datos_abiertos_covid19_30.04.2020.zip"
    ),
}

# Testing data location.
LOCAL_DATA_FILE = (
    Path(__file__).parent / "data" / "datos_abiertos_covid19_31.08.2020.zip"
)

# The following headers are set in true responses from the
# government website.
CONTENT_TYPE = "application/x-zip-compressed"
CONTENT_LENGTH = str(LOCAL_DATA_FILE.stat().st_size)
RESPONSE_HEADERS = {
    "Content-Length": CONTENT_LENGTH,
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
        data.path.unlink()
    assert not data.path.exists()


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
def connection(manager: DataManager):
    """Yield an auto-closing SQLite connection to the system database."""
    with closing(manager.connection) as conn:
        yield conn


def test_not_different_than(
    covid_data: COVIDData, covid_data_info: COVIDDataInfo
):
    """Verify we know when the info of two data sources differ."""
    assert not covid_data_info.different_than(covid_data.info)


def test_chunks(covid_data: COVIDData):
    """Check the expected sizes of the partial dataframes"""
    size = 2 ** 10
    dfs_chunks = covid_data.chunks(size=size)
    dfs_num_rows = []
    for df_chunk in dfs_chunks:
        num_rows = df_chunk.shape[0]
        dfs_num_rows.append(num_rows)
    # All except for the last dataframe must have the same number of rows.
    for num_rows in dfs_num_rows[:-1]:
        assert num_rows == size


def test_save_covid_data(
    covid_data: COVIDData, manager: DataManager, connection: Connection
):
    """Check that we can store COVID data without a problem."""
    # Any required rollback operations are realized inside the
    # save_covid_data method.
    manager.save_covid_data(connection, covid_data)
    con_cursor = connection.cursor()
    sql_query = """
        SELECT name
        FROM sqlite_master
        WHERE type = 'table' AND name NOT LIKE 'sqlite_%;'
    """
    con_cursor.execute(sql_query)
    tables = {table_name for table_name, in con_cursor.fetchall()}
    assert manager.config.COVID_DATA_TABLE_NAME in tables


def test_save_catalogs(manager: DataManager, connection: Connection):
    """Check that other data catalogs can be stored without problems."""
    with connection:
        manager.save_catalogs(connection)
    # Manually check the tables are all in the database.
    con_cursor = connection.cursor()
    sql_query = """
        SELECT name
        FROM sqlite_master
        WHERE type = 'table' AND name NOT LIKE 'sqlite_%;'
    """
    con_cursor.execute(sql_query)
    tables = {table_name for table_name, in con_cursor.fetchall()}
    for cat_file in manager.catalogs():
        assert cat_file.stem.lower() in tables


def test_clean_sources(manager: DataManager):
    """Verify that deletion of COVID-19 data sources works well."""
    manager.clean_sources()
