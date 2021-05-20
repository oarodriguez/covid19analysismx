import os
from contextlib import contextmanager
from pathlib import Path

import pytest
import responses
from duckdb import DuckDBPyConnection, connect

from covid19analysismx import (
    Config,
    COVIDData,
    COVIDDataSpec,
    DataInfo,
    DataManager,
    DBDataManager,
)

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


# ****** Testing starts here. ******


def test_not_different_than(
    covid_data: COVIDData,
    covid_data_info: DataInfo,
):
    """Verify we know when the info of two data sources differ."""
    assert not covid_data_info.different_than(covid_data.info)


def test_spec_not_different_than(
    covid_data_spec: COVIDDataSpec,
    covid_data_spec_info: DataInfo,
):
    """Verify we know when the info of two specs differ."""
    assert not covid_data_spec_info.different_than(covid_data_spec.info)


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


def test_save_covid_data(config: Config, covid_data: COVIDData):
    """Check that we can store COVID data without a problem."""
    # Any required rollback operations are realized inside the
    # save_covid_data method.
    database = config.DATABASE
    table_name = config.COVID_DATA_TABLE_NAME
    if database.exists():
        database.unlink()
    connection = connect(str(database))
    dbd_manager = DBDataManager(connection)
    # Save information in the database.
    dbd_manager.create_covid_cases_table(table_name)
    dbd_manager.save_covid_data(table_name, covid_data)
    # Duckdb database tables are stored into the table
    # sqlite_master, just like in a SQLite database.
    sql_query = """
        SELECT name
        FROM sqlite_master
        WHERE type = 'table' AND name NOT LIKE 'sqlite_%;'
    """
    connection.execute(sql_query)
    tables = {table_name for table_name, in connection.fetchall()}
    assert table_name in tables
    connection.close()


def test_extract_catalogs(config: Config, covid_data_spec: COVIDDataSpec):
    """Verify that we can extract the catalogs information.

    In this test, we only check that we can extract the catalogs successfully
    to CSV files. In principle, these files go into the CATALOGS_DIR
    directory defined in the project configuration. However, the catalogs
    data extracted from the spec file (an Excel spreadsheet) must be fixed
    manually since the spec file has some format inconsistencies. We do not
    want to overwrite already existing, well-formed CSV files with
    inconsistent ones, so we define a different catalogs_dir variable to
    store the new files.
    """
    catalogs_dir = project_path / "__tests_data__" / "catalogs"
    catalogs_dir.mkdir(parents=True, exist_ok=True)
    for cat_name, cat_df in covid_data_spec.catalogs():
        file_name = f"{cat_name}_cat.csv"
        file_name = catalogs_dir / file_name
        cat_df.to_csv(file_name, index=False, line_terminator="\n")
        print(cat_name)


def test_save_catalogs(config: Config, manager: DataManager):
    """Check that other data catalogs can be stored without problems."""
    connection = connect(str(config.DATABASE))
    dbd_manager = DBDataManager(connection)
    dbd_manager.save_catalogs(manager.catalogs())
    # Manually check the tables are all in the database.
    sql_query = """
        SELECT name
        FROM sqlite_master
        WHERE type = 'table' AND name NOT LIKE 'sqlite_%;'
    """
    connection.execute(sql_query)
    tables = {table_name for table_name, in connection.fetchall()}
    for cat_name, _ in manager.catalogs():
        assert cat_name in tables
    connection.close()


def test_clean_sources(manager: DataManager):
    """Verify that deletion of COVID-19 data sources works well."""
    manager.clean_sources(csv_files=True)
