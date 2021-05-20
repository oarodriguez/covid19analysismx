"""Command Line Interface of the project."""

from pathlib import Path
from typing import Optional

import click
import duckdb
import requests
import responses
import typer

from covid19analysismx import (
    Config,
    DataInfo,
    DataManager,
    DBDataManager,
    console,
)

# CLI application instance.

app = typer.Typer()

# Global configuration object.
config = Config.from_environ()
manager = DataManager(config)


@app.command()
def check_data_updates():
    """Check if there is new data available at the remote sources."""
    with console.status("[blue]Checking for updates..."):
        if not manager.covid_data_file.exists():
            console.print(
                "Local COVID-19 data has not been saved/downloaded yet."
            )
            return
        else:
            if manager.covid_data_differ():
                console.print(
                    "[bold]Local COVID-19 data file size does not match the "
                    "remote source file size. It is recommended to download "
                    "the remote data again.[/]"
                )
            else:
                console.print("Local COVID-19 data is up to date.")

        if not manager.covid_data_spec_file.exists():
            console.print(
                "Local COVID-19 data spec file has not been "
                "saved/downloaded yet."
            )
            return
        else:
            if manager.covid_data_specs_differ():
                console.print(
                    "[bold]Local COVID-19 data spec file size does not match "
                    "the remote source file size. It is recommended to "
                    "download the remote data again.[/]"
                )
            else:
                console.print("Local COVID-19 data is up to date.")


force_download_spec = typer.Option(
    False,
    "--force",
    "-f",
    is_flag=True,
    help="Download the remote data, even if an identical local copy exists.",
)


@app.command()
def download_data(force: Optional[bool] = force_download_spec):
    """Download the latest data from the remote servers."""
    # Create the data directory.
    manager.config.DATA_DIR.mkdir(parents=True, exist_ok=True)
    # Download specs data.
    with console.status("[blue]Downloading data..."):
        if force or manager.covid_data_specs_differ():
            console.print(
                "Downloading specs for the COVID-19 data from remote site..."
            )
            specs_data = manager.download_covid_data_spec()
            console.print(f"Specs data have been downloaded and extracted.")
            console.print(
                f"Catalogs data location: {specs_data.catalogs_path}"
            )
            console.print(
                f"Descriptors data location: {specs_data.descriptors_path}"
            )
        else:
            console.print(
                f"The specs local and remote files are identical. "
                f"Skipping download."
            )
        # Download COVID cases data.
        if force or manager.covid_data_differ():
            console.print("Downloading COVID-19 data from remote site...")
            covid_data = manager.download_covid_data()
            console.print(
                f"COVID-19 cases data have been downloaded and extracted."
            )
            print(f"Data location: {covid_data.path}")
        else:
            console.print(
                f"The COVID-19 cases local and remote files are "
                f"identical. Skipping download."
            )


@app.command()
def extract_catalogs():
    """Extract the catalogs from the COVID data specs files."""
    with console.status(
        "Extracting the specs for COVID cases data..."
    ) as status:
        covid_data_spec = manager.extract_covid_data_spec(
            manager.covid_data_spec_file
        )
        catalogs_dir = manager.config.CATALOGS_DIR
        catalogs_dir.mkdir(parents=True, exist_ok=True)
        status.update("Exporting the catalogs...")
        console.print(f"Catalogs directory: {catalogs_dir}")
        for cat_name, cat_df in covid_data_spec.catalogs():
            file_name = f"{cat_name}_cat.csv"
            file_name = catalogs_dir / file_name
            cat_df.to_csv(file_name, index=False, line_terminator="\n")
            console.print(f"  ✅ Catalog '{cat_name}' exported.")
            console.print(f"     Catalog file: {file_name}")
        status.update("✅ Exporting the catalogs")


source_data_spec = typer.Option(
    None,
    "--source-file",
    "-s",
    dir_okay=False,
    help="Use an existing data file located at PATH to set up the database.",
)

skip_cases_spec = typer.Option(
    False,
    is_flag=True,
    help="Omit saving the COVID cases data.",
)


@app.command("setup-db")
def setup_database(
    source_data: Optional[Path] = source_data_spec,
    skip_cases: bool = skip_cases_spec,
):
    """Set up the system database."""
    with console.status("Initializing the system database...") as status:
        # Create the data directory.
        manager.config.DATA_DIR.mkdir(parents=True, exist_ok=True)
        if manager.config.DATABASE.exists():
            manager.config.DATABASE.unlink()
        if source_data is None:
            covid_data_file = manager.covid_data_file
        else:
            if not source_data.is_absolute():
                covid_data_file = (Path.cwd() / source_data).resolve()
            else:
                covid_data_file = source_data.resolve()
        # Save the data to the database.
        db_name = str(config.DATABASE)
        connection = duckdb.connect(db_name)
        dbd_manager = DBDataManager(connection)
        if not skip_cases:
            status.update("Extracting COVID cases data...")
            covid_data = manager.extract_covid_data(covid_data_file)
            console.print("✅ Extracting COVID cases data.")
            # Saving the cases data.
            status.update("Saving COVID-19 cases to the database...")
            dbd_manager.create_covid_cases_table(config.COVID_DATA_TABLE_NAME)
            dbd_manager.save_covid_data(
                config.COVID_DATA_TABLE_NAME, covid_data
            )
            console.print("✅ Saving COVID-19 cases data to the database.")
        # Saving the catalogs.
        status.update("Saving additional information catalogs...")
        dbd_manager.save_catalogs(manager.catalogs())
        console.print("✅ Saving additional information catalogs.")
        # Do not forget to close the connection.
        connection.close()
