"""Routines for retrieving and transforming the project data sources."""

import json
from dataclasses import dataclass
from datetime import date, datetime
from enum import Enum, unique
from pathlib import Path
from typing import Any, ClassVar, Dict, Iterable, Mapping, Optional, Tuple
from urllib.parse import urlparse
from zipfile import ZipFile

import pandas as pd
import requests
from duckdb import DuckDBPyConnection
from requests import Response

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
    INDIGENA = "INDIGENA"
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
    TOMA_MUESTRA_LAB = "TOMA_MUESTRA_LAB"
    RESULTADO_LAB = "RESULTADO_LAB"
    TOMA_MUESTRA_ANTIGENO = "TOMA_MUESTRA_ANTIGENO"
    RESULTADO_ANTIGENO = "RESULTADO_ANTIGENO"
    CLASIFICACION_FINAL = "CLASIFICACION_FINAL"
    MIGRANTE = "MIGRANTE"
    PAIS_NACIONALIDAD = "PAIS_NACIONALIDAD"
    PAIS_ORIGEN = "PAIS_ORIGEN"
    UCI = "UCI"


# NOTE: Never forget to look at https://strftime.org


@dataclass(frozen=True)
class DataInfo:
    """Represent the relevant information about a COVID data source."""

    # The information file contents.
    info: Dict[str, Any]

    @classmethod
    def from_file(cls, path: Path):
        """Post initialization."""
        with path.open("r") as fp:
            info = json.load(fp)
            if "http_headers" in info:
                # NOTE: Is this necessary? We must check.
                info["http_headers"] = normalize_http_headers(
                    info["http_headers"]
                )
        return cls(info)

    @property
    def source_name(self) -> Optional[str]:
        """Return the data file name."""
        return self.info.get("source_file_name", None)

    @property
    def source_date(self):
        """Return the data last update date."""
        # TODO: Consider removing this method.
        _date = self.info.get("source_data_date", None)
        return None if _date is None else date.fromisoformat(_date)

    @property
    def http_headers(self) -> Optional[Dict[str, Any]]:
        """Received HTTP headers when downloading the data."""
        return self.info.get("http_headers", None)

    @property
    def source_data_size(self):
        """Return the data size in bytes."""
        if self.http_headers is None:
            return None
        return int(self.http_headers.get("content-length"))

    def save(self, path: Path):
        """Save a data source's information."""
        with path.open("w") as fp:
            json.dump(self.info, fp, indent=4)

    def different_than(self, other: "DataInfo"):
        """Check if data is different than other object data."""
        # HEAD requests at COVID-19 data sources URL return the
        # Content-Length header. We use this to decide if there is
        # different and newer data available.
        if self.source_data_size is None:
            return True
        if other.source_data_size is None:
            return True
        return (
            True if self.source_data_size != other.source_data_size else False
        )


@dataclass(frozen=True)
class COVIDData:
    """Represent a COVID data source."""

    # Data location.
    path: Path

    # Data info.
    info: DataInfo

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
class COVIDDataSpec:
    """Represent a COVID data spec source."""

    # Descriptors data location.
    descriptors_path: Path

    # Catalogs data location.
    catalogs_path: Path

    # Data info.
    info: DataInfo


def normalize_http_headers(headers: Mapping[str, Any]):
    """Normalize the headers names from an HTTP request.

    :param headers: A mapping with the HTTP headers to normalize.
    :return: A dictionary with the headers, with the header names in
             lowercase.
    """
    norm_headers = {}
    for name, value in headers.items():
        norm_headers[name.lower()] = value
    return norm_headers


# Useful type hints.
CatalogName = str
DataCatalogs = Iterable[Tuple[CatalogName, Path]]


@dataclass(frozen=True)
class DataManager:
    """Manage several process for data manipulation and storage."""

    # Project/App configuration instance.
    config: Config

    def remote_covid_data_info(self):
        """Retrieve information about the latest COVID data."""
        data_url = self.config.COVID_DATA_URL
        response = requests.head(data_url)
        response.raise_for_status()
        headers = normalize_http_headers(response.headers)
        return DataInfo({"http_headers": headers})

    def remote_covid_data_spec_info(self):
        """Retrieve information about the latest COVID data."""
        data_url = self.config.COVID_DATA_SPEC_URL
        response = requests.head(data_url)
        response.raise_for_status()
        headers = normalize_http_headers(response.headers)
        return DataInfo({"http_headers": headers})

    def download_covid_data(self):
        """Retrieve the COVID data from the government website.

        It stores the CSV file with data in the filesystem, and
        discards the zipped version.
        """
        data_url = self.config.COVID_DATA_URL
        latest_resp = requests.get(data_url)
        latest_resp.raise_for_status()
        # We are going to save the data in the DATA_DIR directory.
        zip_file_name = Path(urlparse(data_url).path).name
        dest_dir = self.config.DATA_DIR
        zip_path = dest_dir / zip_file_name
        with zip_path.open("wb") as fp:
            fp.write(latest_resp.content)
        # Store the information file.
        data_path = self.unzip_covid_data_csv(ZipFile(zip_path))
        data_info = self.covid_data_info(data_path, latest_resp)
        info_path = zip_path.with_suffix(".json")
        data_info.save(info_path)
        return COVIDData(data_path, data_info)

    def extract_covid_data(self, path: Path, response: Response = None):
        """Retrieve COVID data from a local zipped file."""
        data_path = self.unzip_covid_data_csv(ZipFile(path))
        # Store the information file.
        data_info = self.covid_data_info(data_path, response)
        info_path = path.with_suffix(".json")
        data_info.save(info_path)
        return COVIDData(data_path, data_info)

    def unzip_covid_data_csv(self, zip_file: ZipFile):
        """Extract the CSV from a zipped data file."""
        file_name = None
        dest_dir = self.config.DATA_DIR
        for file_name in zip_file.namelist():
            if file_name.endswith("COVID19MEXICO.csv"):
                zip_info = zip_file.getinfo(file_name)
                data_path = dest_dir / file_name
                if not data_path.exists():
                    zip_file.extract(file_name, path=dest_dir)
                else:
                    if zip_info.file_size != data_path.stat().st_size:
                        zip_file.extract(file_name, path=dest_dir)
                break
        # Something is wrong with the zip file.
        if file_name is None:
            raise KeyError
        return dest_dir / file_name

    @staticmethod
    def covid_data_info(path: Path, response: Response = None):
        """Retrieve information about a COVID CSV data file."""
        file_name = path.name
        source_name_fmt = "%d%m%yCOVID19MEXICO.csv"
        source_date = datetime.strptime(file_name, source_name_fmt).date()
        file_info: Dict[str, Any] = {
            "source_file_name": file_name,
            "source_data_date": source_date.isoformat(),
        }
        if response is not None:
            file_info["http_headers"] = normalize_http_headers(
                response.headers
            )
        return DataInfo(file_info)

    def download_covid_data_spec(self):
        """Download the file containing the spec of the COVID data."""
        data_url = self.config.COVID_DATA_SPEC_URL
        latest_resp = requests.get(data_url)
        latest_resp.raise_for_status()
        # We are going to save the data in the DATA_DIR directory.
        zip_file_name = Path(urlparse(data_url).path).name
        dest_dir = self.config.DATA_DIR
        zip_path = dest_dir / zip_file_name
        with zip_path.open("wb") as fp:
            fp.write(latest_resp.content)
        return self.extract_covid_data_spec(zip_path, response=latest_resp)

    def extract_covid_data_spec(self, path: Path, response: Response = None):
        """Extract the spreadsheets containing the COVID data spec.

        The zipped file should contain a pair of MS Excel spreadsheets
        with the information.
        """
        data_files = {}
        zip_file = ZipFile(path)
        dest_dir = self.config.DATA_DIR
        for file_name in zip_file.namelist():
            if file_name.endswith(".xlsx"):
                zip_info = zip_file.getinfo(file_name)
                desc_path = dest_dir / file_name
                if not desc_path.exists():
                    zip_file.extract(file_name, path=dest_dir)
                else:
                    if zip_info.file_size != desc_path.stat().st_size:
                        zip_file.extract(file_name, path=dest_dir)
                if file_name.endswith("Descriptores_.xlsx"):
                    data_files["descriptors_file_name"] = dest_dir / file_name
                elif file_name.endswith("Catalogos.xlsx"):
                    data_files["catalogs_file_name"] = dest_dir / file_name
                else:
                    pass
        # Something is wrong with the zip file.
        if not data_files:
            raise KeyError

        # Retrieve information about a COVID spec data files.
        desc_path = data_files["descriptors_file_name"]
        cats_path = data_files["catalogs_file_name"]
        file_name = desc_path.name
        source_name_fmt = "%y%m%d Descriptores_.xlsx"
        source_date = datetime.strptime(file_name, source_name_fmt).date()
        file_info: Dict[str, Any] = {
            "descriptors_file_name": file_name,
            "catalogs_file_name": cats_path.name,
            "source_data_date": source_date.isoformat(),
        }
        if response is not None:
            file_info["http_headers"] = normalize_http_headers(
                response.headers
            )
        # Store the information file.
        data_info = DataInfo(file_info)
        info_path = path.with_suffix(".json")
        data_info.save(info_path)
        return COVIDDataSpec(desc_path, cats_path, data_info)

    def catalogs(self) -> DataCatalogs:
        """Iterate over catalogs, i.e., files with a .csv extension."""
        cat_dir = self.config.CATALOGS_DIR
        for file_path in cat_dir.iterdir():
            if file_path.suffix == ".csv":
                table_name = file_path.stem.lower()
                yield table_name, file_path

    def clean_sources(
        self,
        zip_files: bool = True,
        csv_files: bool = False,
        info_files: bool = False,
    ):
        """Remove data sources inside the data directory.

        This routine only removes files whose names match the following
        glob patterns:

        - *COVID19MEXICO.csv
        - *COVID19MEXICO.json
        - datos_abiertos_covid19*.zip

        Also, it only deletes those files if their corresponding flags
        (zip_files, csv_files, and  info_files, respectively) are True.
        """
        sources_dir = self.config.DATA_DIR
        assert sources_dir.is_dir()
        assert sources_dir.exists()

        def _unlink(_child: Path):
            """Delete the file ``_child``."""
            if _child.is_file():
                _child.unlink()

        if zip_files:
            for child in sources_dir.glob("datos_abiertos_covid19*.zip"):
                _unlink(child)
        if csv_files:
            for child in sources_dir.glob("*COVID19MEXICO.csv"):
                _unlink(child)
        if info_files:
            for child in sources_dir.glob("*COVID19MEXICO.json"):
                _unlink(child)


@dataclass(frozen=True)
class DBDataManager:
    """Handle the tasks for storing data in the database."""

    # The connection object to the system database.
    connection: DuckDBPyConnection

    def create_covid_cases_table(self, table_name: str):
        """Create the COVID-19 data table in the system database."""
        query = f"""
        CREATE TABLE {table_name}
        (
            FECHA_ACTUALIZACION   DATE,
            ID_REGISTRO           TEXT,
            ORIGEN                INTEGER,
            SECTOR                INTEGER,
            ENTIDAD_UM            INTEGER,
            SEXO                  INTEGER,
            ENTIDAD_NAC           INTEGER,
            ENTIDAD_RES           INTEGER,
            MUNICIPIO_RES         INTEGER,
            TIPO_PACIENTE         INTEGER,
            FECHA_INGRESO         DATE,
            FECHA_SINTOMAS        DATE,
            FECHA_DEF             TEXT,
            INTUBADO              INTEGER,
            NEUMONIA              INTEGER,
            EDAD                  INTEGER,
            NACIONALIDAD          INTEGER,
            EMBARAZO              INTEGER,
            HABLA_LENGUA_INDIG    INTEGER,
            INDIGENA              INTEGER,
            DIABETES              INTEGER,
            EPOC                  INTEGER,
            ASMA                  INTEGER,
            INMUSUPR              INTEGER,
            HIPERTENSION          INTEGER,
            OTRA_COM              INTEGER,
            CARDIOVASCULAR        INTEGER,
            OBESIDAD              INTEGER,
            RENAL_CRONICA         INTEGER,
            TABAQUISMO            INTEGER,
            OTRO_CASO             INTEGER,
            TOMA_MUESTRA_LAB      INTEGER,
            RESULTADO_LAB         INTEGER,
            TOMA_MUESTRA_ANTIGENO INTEGER,
            RESULTADO_ANTIGENO    INTEGER,
            CLASIFICACION_FINAL   INTEGER,
            MIGRANTE              INTEGER,
            PAIS_NACIONALIDAD     TEXT,
            PAIS_ORIGEN           TEXT,
            UCI                   INTEGER
        );
        """
        # Create the COVID cases table according to the definition.
        self.connection.execute(query)

    def save_covid_data(self, table_name: str, data: COVIDData):
        """Save the COVID-19 data into the database."""
        # Transfer the data from the CSV file into the database in bulk.
        # Limit the amount of memory used during the bulk insertion to
        # 2 Gigabytes.
        data_path = data.path
        query = f"""
            PRAGMA memory_limit='2.0GB';
            COPY "{table_name}"
            FROM '{data_path}' ( HEADER );
        """
        self.connection.execute(query)

    def save_catalog(self, name: str, path: Path):
        """Save the catalog data in the database.

        If the catalogs data already exist in the database, the
        corresponding tables will be deleted and recreated.
        """
        # Here, since the only characters in the catalogs file names
        # are alphanumeric (we saved the files this way on purpose), we
        # use the file name, without the extension, as the table name .
        # Naturally, we convert the name the lowercase.
        cat_data = pd.read_csv(path)
        self.connection.register("cat_data_view", cat_data)
        query = f"""
                DROP TABLE IF EXISTS {name};
                CREATE TABLE {name} AS
                SELECT * FROM cat_data_view;
            """
        self.connection.unregister("cat_data_view")
        self.connection.execute(query)

    def save_catalogs(self, catalogs: DataCatalogs):
        """Save the catalogs data in the system database."""
        for name, path in catalogs:
            # Here, since the only characters in the catalogs file names
            # are alphanumeric (we saved the files this way on purpose), we
            # use the file name, without the extension, as the table name .
            # Naturally, we convert the name the lowercase.
            self.save_catalog(name, path)
