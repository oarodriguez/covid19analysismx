"""Command Line Interface of the project."""
# from __future__ import annotations

from typing import Optional

import duckdb
import typer

# CLI application instance.
from covid19analysismx import Config, COVIDDataInfo, DataManager, DBDataManager

app = typer.Typer()

# Global configuration object.
config = Config.from_environ()
manager = DataManager(config)

force_spec = typer.Option(
    False,
    "--force",
    "-f",
    help="Force the initialization of the database. If the database "
    "is already initialized, then its data will be replaced.",
)


@app.command()
def setup_database(force: Optional[bool] = force_spec):
    """Set up the system database.

    Download the most recent COVID-19 data if necessary, and store its
    contents into a new DuckDB database instance. The new database is named
    according to the data date, so the main project database is not replaced
    immediately.
    """
    print("Initializing system database...")
    saved_data_info_file = manager.config.DATABASE.with_suffix(
        manager.config.SAVED_COVID_DATA_INFO_FILE_SUFFIX
    )
    if saved_data_info_file.exists():
        saved_data_info = COVIDDataInfo.from_file(saved_data_info_file)
        remote_info = manager.remote_covid_data_info()
        if not saved_data_info.different_than(remote_info):
            if not force:
                print("Local COVID-19 data is up to date.")
                raise typer.Exit

    # Create the data directory.
    manager.config.DATA_DIR.mkdir(parents=True, exist_ok=True)

    # Download the data.
    print("Downloading latest COVID-19 data from remote site...")
    data = manager.download_covid_data(keep_zip=True)
    print("COVID-19 data downloaded.")

    # SQLite connection. It should be automatically closed on
    connection = duckdb.connect(str(config.DATABASE))
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
