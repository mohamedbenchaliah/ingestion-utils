import re

from pyspark.sql.functions import current_timestamp, to_timestamp

from ingestor.tools import get_spark_env
from ingestor.tools import Log


_logger = Log()


class TableLoader:

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
            self,
            source_project: str,
            source_dataset: str,
            target_project: str,
            target_dataset: str,
            target_table: str,
            file: str,
            temporary_gcs_bucket: str,
            materialization_dataset: str
    ):

        self._source_project = source_project
        self._source_dataset: str = source_dataset
        self._target_project = target_project
        self._target_dataset: str = target_dataset
        self._target_table: str = target_table
        self._file_path: str = file
        self._temporary_gcs_bucket: str = temporary_gcs_bucket
        self._materialization_dataset: str = materialization_dataset

    @staticmethod
    def fullmatch(regex: str, string: str, flags=0):
        """fullmatch function Emulate python-3.4 re.fullmatch()."""

        return re.match("(?:" + regex + r")\Z", string, flags=flags)

    def load(self) -> None:
        """Read a view from BigQuery and write it as a BigQuery table."""

        _logger.info("Starting Spark application ...")
        spark = get_spark_env(
            project=self._target_project,
            temporary_gcs_bucket=self._temporary_gcs_bucket,
            materialization_dataset=self._materialization_dataset,
            expiration_time=30,
            view_enabled=True,
        )

        _logger.info(
            "Start processing table `{}.{}` ... ".format(
                self._target_project,
                self._target_dataset,
                self._target_table
            )
        )

        try:
            sql_path = self._file_path
            with open(sql_path, "r") as sql:
                query_text = sql.read()
                query_text = query_text.format(
                    project_id=self._source_project,
                    dataset_id=self._source_dataset
                )

            _logger.info("Reading Bigquery query result into a Spark Dataframe")

            df = spark.read.format("bigquery").load(query_text)

            df.createOrReplaceTempView("vw_{}_tmp".format(self._target_table))
            count = spark.sql("SELECT count(*) as cnt FROM vw_{}_tmp".format(self._target_table)).first()["cnt"]

            _logger.info("Reading {cnt} rows into a Spark Dataframe".format(cnt=count))

            df = df.withColumn("CREATION_DATE", to_timestamp(current_timestamp()))
            df = df.withColumn("UPDATE_DATE", to_timestamp(current_timestamp()))

            _logger.info(
                "Writing Spark DF into Bigquery table `{}.{}`".format(self._target_dataset, self._target_table)
            )
            (
                df.write.format("bigquery")
                .mode("overwrite")
                .save("{}.{}.{}".format(self._target_project,self._target_dataset, self._target_table))
            )

            _logger.info(
                "Successfully loaded the table `{}.{}.{}`".format(
                    self._target_project,
                    self._target_dataset,
                    self._target_table
                )
            )

        except Exception as e:
            _logger.error(
                "Unexpected error while loading the table `{}.{}.{}`. ".format(
                    self._target_project,
                    self._target_dataset,
                    self._target_table
                )
            )
            raise e
