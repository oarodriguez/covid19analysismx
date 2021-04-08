"""Set configurations variables for the project."""

import os
from dataclasses import dataclass
from pathlib import Path

from dotenv import dotenv_values

# Exported variables.
__all__ = ["Config"]

# Environment variables.
DEFAULT_DOTENV_FILE = ".env"
# Used to override the name of the .env file loaded by python-dotenv.
DOTENV_FILE = "DOTENV_FILE"

# Constants.
DATA_DIR = "data"
CATALOGS_DIR_NAME = "catalogs"
DATABASE_NAME = "main-database.sqlite"

# URL for getting the most recent data.
COVID_DATA_URL = (
    "http://datosabiertos.salud.gob.mx/gobmx/salud/"
    "datos_abiertos/datos_abiertos_covid19.zip"
)


def get_environ():
    """Get environment variables.

    The returned dictionary include the variables loaded by python-dotenv.
    The system environment variables are not overridden.
    """
    environ = {}
    dotenv_file = os.getenv(DOTENV_FILE, DEFAULT_DOTENV_FILE)
    # TODO: Allow override system environment variables?
    environ.update(dotenv_values(dotenv_file), **os.environ)
    return environ


@dataclass(frozen=True)
class Config:
    """Groups the main configuration variables of the project."""

    # Data directory.
    DATA_DIR: Path

    # Database name.
    DATABASE: Path

    # Directory where other data catalogs are located.
    CATALOGS_DIR: Path

    # Parsed URL for getting the most recent COVID data.
    COVID_DATA_URL: str

    # Name for the COVID-19 table.
    COVID_DATA_TABLE_NAME: str = "covid_cases"

    @classmethod
    def from_environ(cls):
        """Initialize from the system environment variables."""
        environ = get_environ()

        # Normalize the data directory.
        data_dir_var = environ.get("DATA_DIR")
        if data_dir_var is None:
            raise KeyError(
                "the required environment variable 'DATA_DIR' is not set"
            )
        data_dir = Path(data_dir_var).expanduser().resolve()

        # Normalize the catalogs directory.
        catalogs_dir_var = environ.get("CATALOGS_DIR")
        catalogs_dir = (
            data_dir / CATALOGS_DIR_NAME
            if catalogs_dir_var is None
            else Path(catalogs_dir_var).expanduser().resolve()
        )

        # The full URI to access the database.
        database_var = environ.get("DATABASE")
        database = (
            data_dir / DATABASE_NAME
            if database_var is None
            else Path(database_var).expanduser().resolve()
        )

        # Latest data URL.
        covid_data_url = environ.get("COVID_DATA_URL", COVID_DATA_URL)

        # Set up a new configuration instance.
        return cls(
            DATA_DIR=data_dir,
            DATABASE=database,
            CATALOGS_DIR=catalogs_dir,
            COVID_DATA_URL=covid_data_url,
        )
