import os
from contextlib import contextmanager
from pathlib import Path

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
    yield manager.connect()


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


def test_save_covid_data(covid_data: COVIDData, manager: DataManager):
    """Check that we can store COVID data without a problem."""
    # Any required rollback operations are realized inside the
    # save_covid_data method.
    if manager.config.DATABASE.exists():
        manager.config.DATABASE.unlink()
    connection = manager.connect()
    manager.create_covid_cases_table(connection)
    manager.save_covid_data(connection, covid_data)
    # Duckdb database tables are stored into the table
    # sqlite_master, just like in a SQLite database.
    sql_query = """
        SELECT name
        FROM sqlite_master
        WHERE type = 'table' AND name NOT LIKE 'sqlite_%;'
    """
    connection.execute(sql_query)
    tables = {table_name for table_name, in connection.fetchall()}
    assert manager.config.COVID_DATA_TABLE_NAME in tables
    connection.close()


def test_save_catalogs(manager: DataManager):
    """Check that other data catalogs can be stored without problems."""
    connection = manager.connect()
    manager.save_catalogs(connection)
    # Manually check the tables are all in the database.
    sql_query = """
        SELECT name
        FROM sqlite_master
        WHERE type = 'table' AND name NOT LIKE 'sqlite_%;'
    """
    connection.execute(sql_query)
    tables = {table_name for table_name, in connection.fetchall()}
    for cat_file in manager.catalogs():
        assert cat_file.stem.lower() in tables
    connection.close()


def test_clean_sources(manager: DataManager):
    """Verify that deletion of COVID-19 data sources works well."""
    manager.clean_sources(csv_files=True)
