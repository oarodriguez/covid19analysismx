"""Command Line Interface of the project."""

from pathlib import Path
from typing import Optional

import click
import duckdb
import requests
import responses
import typer

from covid19analysismx import Config, COVIDDataInfo, DataManager, DBDataManager

# CLI application instance.
app = typer.Typer()

# Global configuration object.
config = Config.from_environ()
manager = DataManager(config)

force_spec = typer.Option(
    False,
    "--force",
    "-f",
    is_flag=True,
    help="Force the initialization of the database. If the database "
    "is already initialized, then its data will be replaced.",
)

cached_data_spec = typer.Option(
    None,
    "--cached-data",
    "-c",
    dir_okay=False,
    help="Use an existing data source located at PATH to set up the "
    "database. The configuration DATA_DIR directory will be used "
    "as the base path if PATH is a relative path.",
)


@app.command("setup-db")
def setup_database(
    force: Optional[bool] = force_spec,
    cached_data: Optional[Path] = cached_data_spec,
):
    """Set up the system database.

    Download the most recent COVID-19 data if necessary, and store its
    contents into a new DuckDB database instance. The new database is named
    according to the data date, so the main project database is not replaced
    immediately.
    """
    saved_data_info_file = manager.config.DATABASE.with_suffix(
        manager.config.SAVED_COVID_DATA_INFO_FILE_SUFFIX
    )
    if cached_data is None:
        if saved_data_info_file.exists():
            saved_data_info = COVIDDataInfo.from_file(saved_data_info_file)
            # Only download new data if it is different than the
            # current one. Otherwise, finish the program.
            mock_info = manager.remote_covid_data_info()
            if not saved_data_info.different_than(mock_info):
                if not force:
                    print("Local COVID-19 data is up to date.")
                    raise typer.Exit

    print("Initializing system database...")

    # Create the data directory.
    manager.config.DATA_DIR.mkdir(parents=True, exist_ok=True)

    if cached_data is None:
        # Download the data.
        print("Downloading latest COVID-19 data from remote site...")
        data = manager.download_covid_data(keep_zip=True)
        print("COVID-19 data downloaded.")
    else:
        # Use cached data.
        cached_data_path = (config.DATA_DIR / cached_data).resolve()
        if not cached_data_path.exists():
            raise click.ClickException(
                f"file does not exists: {cached_data_path}"
            )
        print(f"Using COVID-19 data located at {cached_data_path}")
        cached_file_name = cached_data_path.name.lower()
        # Use a RequestsMock object in order to get extra information
        # that comes in the HTTP headers.
        with responses.RequestsMock() as req_mock:
            content_type = "application/x-zip-compressed"
            content_length = str(cached_data_path.stat().st_size)
            response_headers = {
                "Content-Length": content_length,
                "Accept-Ranges": "bytes",
            }
            req_mock.add(
                method=responses.HEAD,
                url=config.COVID_DATA_URL,
                body=b"",
                headers=response_headers,
                content_type=content_type,
            )
            response = requests.head(config.COVID_DATA_URL)
            response.raise_for_status()
        if cached_file_name.endswith(".zip"):
            data = manager.extract_covid_data(cached_data_path, response)
        else:
            raise click.ClickException(
                f"cached data file is invalid: {cached_data_path}"
            )

    # We want to set up a new database (and its corresponding information
    # file) with the most recent data. If a database already exists
    # together with an information file, then we will rename the database
    # and the info file according to the CSV source file name used to
    # initialize them.
    if saved_data_info_file.exists():
        saved_data_info = COVIDDataInfo.from_file(saved_data_info_file)
        print(data.info)
        if not saved_data_info.different_than(data.info):
            if not force:
                print("Local COVID-19 data is up to date.")
                raise typer.Exit

        source_name = saved_data_info.source_name
        if config.DATABASE.exists():
            old_db_path = config.DATABASE.with_name(source_name).with_suffix(
                ".duckdb"
            )
            # Rename the current database.
            config.DATABASE.replace(old_db_path)
        old_data_info_path = saved_data_info_file.with_name(
            source_name
        ).with_suffix(".json")
        # Rename the current information file.
        saved_data_info_file.replace(old_data_info_path)

    else:
        # Delete orphaned databases.
        if config.DATABASE.exists():
            config.DATABASE.unlink()

    # Finally, save the latest data to the database.
    db_name = str(config.DATABASE)
    connection = duckdb.connect(db_name)
    dbd_manager = DBDataManager(connection)
    print("Saving COVID-19 data to system database...")
    dbd_manager.create_covid_cases_table(config.COVID_DATA_TABLE_NAME)
    dbd_manager.save_covid_data(config.COVID_DATA_TABLE_NAME, data)
    print("COVID-19 data saved.")
    print("Saving additional information catalogs...")
    dbd_manager.save_catalogs(manager.catalogs())
    print("Catalogs saved.")
    connection.close()

    # Save the saved COVID data information.
    data.info.save(saved_data_info_file)
    print("Database initialized successfully.")


@app.command()
def check_updates():
    """Check if there is new data available at the remote sources."""
    print("Checking for updates...")
    saved_data_info_file = manager.config.DATABASE.with_suffix(
        manager.config.SAVED_COVID_DATA_INFO_FILE_SUFFIX
    )
    if not saved_data_info_file.exists():
        print("Local COVID-19 data has not been saved/downloaded yet.")
        return
    else:
        saved_data_info = COVIDDataInfo.from_file(saved_data_info_file)
        remote_info = manager.remote_covid_data_info()
        if saved_data_info.different_than(remote_info):
            print("There is new data available in the remote sources.")
        else:
            print("Local COVID-19 data is up to date.")
