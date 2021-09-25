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

# Project directory path.
project_path = Path(__file__).parent.parent.parent

# Constants.
DATA_DIR = "data"
CACHE_DIR_NAME = ".cache"
CATALOGS_DIR_NAME = "catalogs"
DATABASE_NAME = "main-database.duckdb"

# URL for getting the most recent data.
COVID_DATA_URL = (
    "http://datosabiertos.salud.gob.mx/gobmx/salud/"
    "datos_abiertos/datos_abiertos_covid19.zip"
)

# URL for getting the most recent catalogs data.
COVID_DATA_SPEC_URL = (
    "http://datosabiertos.salud.gob.mx/gobmx/salud/datos_abiertos/"
    "diccionario_datos_covid19.zip"
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

    # Parsed URL for getting the most recent COVID data.
    COVID_DATA_URL: str

    # URL for getting the most recent catalogs data.
    COVID_DATA_SPEC_URL: str

    # Name for the COVID-19 table.
    COVID_DATA_TABLE_NAME: str = "covid_cases"

    # Suffix for the info file corresponding to the COVID
    # data saved in the database.
    SAVED_COVID_DATA_INFO_FILE_SUFFIX: str = ".saved-covid-data.json"

    @classmethod
    def from_environ(cls):
        """Initialize from the system environment variables."""
        environ = get_environ()

        # Normalize the data directory.
        data_dir_var = environ.get("DATA_DIR")
        data_dir = (
            project_path / DATA_DIR
            if data_dir_var is None
            else Path(data_dir_var).expanduser().resolve()
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

        # Latest catalogs data URL.
        covid_data_spec_url = environ.get(
            "COVID_DATA_SPEC_URL", COVID_DATA_SPEC_URL
        )

        # Set up a new configuration instance.
        return cls(
            DATA_DIR=data_dir,
            DATABASE=database,
            COVID_DATA_URL=covid_data_url,
            COVID_DATA_SPEC_URL=covid_data_spec_url,
        )

    @property
    def cache_dir(self):
        """Location containing cached data files."""
        return self.DATA_DIR / CACHE_DIR_NAME

    @property
    def catalogs_dir(self):
        """Location containing data catalogs."""
        return self.DATA_DIR / CATALOGS_DIR_NAME
