"""Routines for retrieving and transforming the project data sources."""

import json
from dataclasses import dataclass
from datetime import date, datetime
from enum import Enum, unique
from pathlib import Path
from sqlite3 import Connection, connect
from tempfile import TemporaryFile
from typing import Any, ClassVar, Dict, Iterable, Optional
from zipfile import ZipFile

import pandas as pd
import requests

from .config import Config


@unique
class COVIDDataColumn(str, Enum):
    """Gather the columns names in the COVID-19 table.

    This mixin Enum let us to use its members as regular strings,
    for instance, for retrieve data from dataframes of dictionaries.
    """

    # NOTE: Should we use column names in english?
    FECHA_ACTUALIZACION = "FECHA_ACTUALIZACION"
    ID_REGISTRO = "ID_REGISTRO"
    ORIGEN = "ORIGEN"
    SECTOR = "SECTOR"
    ENTIDAD_UM = "ENTIDAD_UM"
    SEXO = "SEXO"
    ENTIDAD_NAC = "ENTIDAD_NAC"
    ENTIDAD_RES = "ENTIDAD_RES"
    MUNICIPIO_RES = "MUNICIPIO_RES"
    TIPO_PACIENTE = "TIPO_PACIENTE"
    FECHA_INGRESO = "FECHA_INGRESO"
    FECHA_SINTOMAS = "FECHA_SINTOMAS"
    FECHA_DEF = "FECHA_DEF"
    INTUBADO = "INTUBADO"
    NEUMONIA = "NEUMONIA"
    EDAD = "EDAD"
    NACIONALIDAD = "NACIONALIDAD"
    EMBARAZO = "EMBARAZO"
    HABLA_LENGUA_INDIG = "HABLA_LENGUA_INDIG"
    DIABETES = "DIABETES"
    EPOC = "EPOC"
    ASMA = "ASMA"
    INMUSUPR = "INMUSUPR"
    HIPERTENSION = "HIPERTENSION"
    OTRA_COM = "OTRA_COM"
    CARDIOVASCULAR = "CARDIOVASCULAR"
    OBESIDAD = "OBESIDAD"
    RENAL_CRONICA = "RENAL_CRONICA"
    TABAQUISMO = "TABAQUISMO"
    OTRO_CASO = "OTRO_CASO"
    RESULTADO = "RESULTADO"
    MIGRANTE = "MIGRANTE"
    PAIS_NACIONALIDAD = "PAIS_NACIONALIDAD"
    PAIS_ORIGEN = "PAIS_ORIGEN"
    UCI = "UCI"


# NOTE: Never forget to look at https://strftime.org


@dataclass(frozen=True)
class COVIDDataInfo:
    """Represent the relevant information about a COVID data source."""

    # The information file contents.
    info: Dict[str, Any]

    @classmethod
    def from_file(cls, path: Path):
        """Post initialization."""
        with path.open("r") as fp:
            info = json.load(fp)
        return cls(info)

    @property
    def source_name(self) -> Optional[str]:
        """Return the data file name."""
        return self.info.get("source_file_name", None)

    @property
    def source_date(self):
        """Return the data last update date."""
        _date = self.info.get("source_file_date", None)
        return None if _date is None else date.fromisoformat(_date)

    @property
    def http_headers(self) -> Dict[str, Any]:
        """Received HTTP headers when downloading the data."""
        return self.info["http_headers"]

    def save(self, path: Path):
        """Save a data source's information."""
        with path.open("w") as fp:
            json.dump(self.info, fp)

    def different_than(self, other: "COVIDDataInfo"):
        """Check if data is different than other object data."""
        # HEAD requests at COVID-19 data sources URL return the
        # Content-Length header. We use this to decide if there is
        # different and newer data available.
        self_size = int(self.http_headers["Content-Length"])
        other_size = int(other.http_headers["Content-Length"])
        return True if self_size != other_size else False


@dataclass(frozen=True)
class COVIDData:
    """Represent a COVID data source."""

    # Data location.
    path: Path

    # Data info.
    info: COVIDDataInfo

    # Default chunk size.
    default_chunk_size: ClassVar[int] = 2 ** 15

    def __post_init__(self):
        """Post initialization."""
        pass

    @staticmethod
    def _fix(dataframe: pd.DataFrame):
        """Fix the columns values.

        For columns with datetime values, we only want to save the date part.
        """
        dataframe[COVIDDataColumn.FECHA_ACTUALIZACION] = dataframe[
            COVIDDataColumn.FECHA_ACTUALIZACION
        ].dt.date
        dataframe[COVIDDataColumn.FECHA_INGRESO] = dataframe[
            COVIDDataColumn.FECHA_INGRESO
        ].dt.date
        dataframe[COVIDDataColumn.FECHA_SINTOMAS] = dataframe[
            COVIDDataColumn.FECHA_SINTOMAS
        ].dt.date
        dataframe[COVIDDataColumn.FECHA_DEF] = dataframe[
            COVIDDataColumn.FECHA_DEF
        ].dt.date
        return dataframe

    def chunks(self, size: int = None):
        """Read the CSV data file in chunks.

        The CSV data file is several hundred megabytes in size, so we
        load it in chunks of a specific size.
        """
        size = size or self.default_chunk_size
        if self.path.exists():
            df_iterator: Iterable[pd.DataFrame]
            df_iterator = pd.read_csv(
                self.path,
                iterator=True,
                chunksize=size,
                parse_dates=[
                    COVIDDataColumn.FECHA_ACTUALIZACION,
                    COVIDDataColumn.FECHA_INGRESO,
                    COVIDDataColumn.FECHA_SINTOMAS,
                    COVIDDataColumn.FECHA_DEF,
                ],
                na_values={COVIDDataColumn.FECHA_DEF: "9999-99-99"},
            )
            for chunk_df in df_iterator:
                yield self._fix(chunk_df)


@dataclass(frozen=True)
class DataManager:
    """Manage several process for data manipulation and storage."""

    # Project/App configuration instance.
    config: Config

    @property
    def connection(self):
        """Return a new connection to the system database."""
        return connect(self.config.DATABASE)

    def remote_covid_data_info(self):
        """Retrieve information about the latest COVID data."""
        data_url = self.config.COVID_DATA_URL
        response = requests.head(data_url)
        response.raise_for_status()
        headers = dict(response.headers)
        return COVIDDataInfo({"http_headers": headers})

    def download_covid_data(self):
        """Get the data from the government website.

        It stores the CSV file with data in the filesystem, and
        discards the zipped version.
        """
        data_url = self.config.COVID_DATA_URL
        latest_resp = requests.get(data_url)
        latest_resp.raise_for_status()
        # We are going to save the data in the DATA_DIR directory.
        file_name = None
        dest_dir = self.config.DATA_DIR
        with TemporaryFile(suffix="zip") as temp_file:
            temp_file.write(latest_resp.content)
            zip_file = ZipFile(temp_file)
            for file_name in zip_file.namelist():
                if file_name.endswith("COVID19MEXICO.csv"):
                    zip_file.extract(file_name, path=dest_dir)
                    break
        # Something is wrong with the zip file.
        if file_name is None:
            raise KeyError
        source_name_fmt = "%d%m%yCOVID19MEXICO.csv"
        source_date = datetime.strptime(file_name, source_name_fmt).date()
        file_info = {
            "source_file_name": file_name,
            "source_file_date": source_date.isoformat(),
            "http_headers": dict(latest_resp.headers),
        }
        data_path = dest_dir / file_name
        info_path = data_path.with_suffix(".json")
        data_info = COVIDDataInfo(file_info)
        # Store the information file
        data_info.save(info_path)
        return COVIDData(data_path, data_info)

    def save_covid_data_chunk(
        self,
        connection: Connection,
        chunk_df: pd.DataFrame,
        save_mode: str = "replace",
    ):
        """Save a COVID-19 data chunk into the system database."""
        table_name = self.config.COVID_DATA_TABLE_NAME
        # save_mode = "replace" if first_chunk else "append"
        chunk_df.to_sql(table_name, connection, if_exists=save_mode)

    def save_covid_data(self, connection: Connection, data: COVIDData):
        """Save the COVID-19 data into the system database.

        If there is data previously stored in the table ``table_name``, this
        method replaces it with the new data.
        """
        data_chunks = data.chunks()
        # When saving the first partial dataframe, we have to indicate
        # that we want to replace any existing data in the table.
        first_chunk = next(data_chunks)
        with connection as conn:
            self.save_covid_data_chunk(conn, first_chunk, save_mode="replace")
        for df_chunk in data_chunks:
            with connection as conn:
                self.save_covid_data_chunk(conn, df_chunk, save_mode="append")

    def catalogs(self):
        """Iterate over catalogs, i.e., files with a .csv extension."""
        cat_dir = self.config.CATALOGS_DIR
        for file in cat_dir.iterdir():
            if file.suffix == ".csv":
                yield file

    @staticmethod
    def save_catalog(catalog: Path, connection: Connection):
        """Save the catalog data in the system database."""
        # Here, since the only characters in the catalogs file names
        # are alphanumeric (we saved the files this way on purpose), we
        # use the file name, without the extension, as the table name .
        # Naturally, we convert the name the lowercase.
        table_name = catalog.stem.lower()
        cat_data = pd.read_csv(catalog)
        cat_data.to_sql(table_name, connection, if_exists="replace")

    def save_catalogs(self, connection: Connection):
        """Save the catalogs data in the system database."""
        for catalog in self.catalogs():
            # Here, since the only characters in the catalogs file names
            # are alphanumeric (we saved the files this way on purpose), we
            # use the file name, without the extension, as the table name .
            # Naturally, we convert the name the lowercase.
            with connection as conn:
                # Rollback if something bad happened.
                self.save_catalog(catalog, conn)

    def clean_sources(self, info: bool = False):
        """Remove data sources (CSV files) inside the data directory.

        The files to be deleted have names that end with
        ``COVID19MEXICO.csv``.
        """
        sources_dir = self.config.DATA_DIR
        assert sources_dir.is_dir()
        assert sources_dir.exists()
        for child in sources_dir.iterdir():
            if not child.is_file():
                continue
            if child.name.endswith("COVID19MEXICO.csv"):
                child.unlink()
                info_file = child.with_suffix(".json")
                if info and info_file.exists():
                    info_file.unlink()
