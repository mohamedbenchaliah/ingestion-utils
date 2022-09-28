import json
import os
import io

from typing import List

import pandas as pd

from google.cloud import bigquery
from google.api_core.exceptions import BadRequest

from ingestor.tools import Log

_logger = Log()
_CLIENT = bigquery.Client()


class FileDataLoader:  # pragma: no cover

    _BQ_DATA_TYPES = [
        "STRING",
        "DATE",
        "TIMESTAMP",
        "INTEGER",
        "BOOL",
        "FLOAT64",
        "NUMERIC",
        "DATETIME",
        "STRUCT",
        "TIME",
        "TIMESTAMP",
        "RECORD",
    ]

    def __init__(
        self, target_project: str, target_dataset: str, target_table: str, data_file_path: str, schema_file_path: str
    ):
        """Class responsible for updating metadata of Bigquery tables.

        Parameters
        ----------
        target_project : STRING
            target project ID
        target_dataset : STRING
            target dataset ID
        target_table : STRING
            target table ID
        data_file_path : STRING
            CSV file path
        schema_file_path : STRING
            Json file path
        Returns
        -------
        None
        """
        self._target_project: str = target_project if target_project else ""
        self._target_dataset: str = target_dataset
        self._target_table: str = target_table
        self._data_file_path: str = data_file_path
        self._schema_file_path: str = schema_file_path

    @staticmethod
    def _get_job_config(ext):
        if ext == ".csv":
            return bigquery.LoadJobConfig(
                source_format=bigquery.SourceFormat.CSV,
                skip_leading_rows=1,
                write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
            )
        elif ext == ".json":
            return bigquery.LoadJobConfig(
                source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON,
                write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
            )
        elif ext == ".orc":
            return bigquery.LoadJobConfig(
                source_format=bigquery.SourceFormat.ORC,
                write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE
            )
        elif ext == ".parquet":
            return bigquery.LoadJobConfig(
                source_format=bigquery.SourceFormat.PARQUET,
                write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE
            )
        elif ext == ".avro":
            return bigquery.LoadJobConfig(
                source_format=bigquery.SourceFormat.AVRO,
                write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE
            )
        else:
            raise TypeError("wrong type of file {} must be either csv or json".format(ext))

    @staticmethod
    def _get_data(data_folder: str = None) -> List[str]:
        """Return all data paths."""

        if data_folder is None:
            return [os.path.join(project_dir) for project_dir in os.listdir(".")]
        else:
            return [os.path.join(data_folder)]

    def launch_send(self, data_file, job_config, schema_file_path):
        with open(schema_file_path) as schema_file:
            fields = json.load(schema_file)
        [_logger.info(field) for field in fields]
        job_config.schema = [
            bigquery.SchemaField(
                field["name"],
                field_type=field.get("type", "STRING"),
                mode=field.get("mode", "NULLABLE"),
                description=field.get("description", ""),
            )
            for field in fields
        ]

        job = _CLIENT.load_table_from_file(
            data_file,
            "{}.{}.{}".format(
                self._target_project,
                self._target_dataset,
                self._target_table
            ),
            job_config=job_config
        )
        try:
            job.result()
        except BadRequest as ex:
            for err in ex.errors:
                _logger.error("Error while uploading CSV to Bigquery table : {}".format(err))
            raise

    def _load_table(
        self,
        data_file_path: str,
        schema_file_path: str,
        ext: str,
    ) -> None:
        """function load a csv into BQ given a schema.json file."""

        if ext == ".json":
            data = json.load(io.open(data_file_path))
            df = pd.DataFrame.from_records(data)
            cv_file = data_file_path.replace("json", "csv")
            df.to_csv(cv_file, encoding="utf-8", index=False, sep=",")
            job_config = self._get_job_config(".csv")
            with open(cv_file, "rb") as data_file:
                self.launch_send(data_file, job_config, schema_file_path)
        else:
            job_config = self._get_job_config(ext)
            with open(data_file_path, "rb") as data_file:
                self.launch_send(data_file, job_config, schema_file_path)

    def load_file_data(self):
        """Main function to load ga metadata to Bigquery."""

        projects = self._get_data("./data")
        _logger.info("looking for file to load")
        for data_dir in projects:
            for root, dirs, files in os.walk(data_dir):
                for filename in files:
                    (base, ext) = os.path.splitext(filename)
                    if filename == self._data_file_path and ext in (".json", ".csv"):
                        schema_file_path = (
                            os.path.join(root, self._schema_file_path) if self._schema_file_path in files else None
                        )
                        self._load_table(
                            data_file_path=os.path.join(root, filename),
                            schema_file_path=schema_file_path,
                            ext=ext
                        )
