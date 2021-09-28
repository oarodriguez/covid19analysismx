"""Verify the routines in the ``covid19mx.data`` module."""

from duckdb import connect

from covid19mx import (
    Config,
    COVIDData,
    COVIDDataSpec,
    DataInfo,
    DataManager,
    DBDataManager,
)


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
    """Check the expected sizes of the partial dataframes."""
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
    table_name = config.covid_data_table_name
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
    catalogs_dir = config.catalogs_dir
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
