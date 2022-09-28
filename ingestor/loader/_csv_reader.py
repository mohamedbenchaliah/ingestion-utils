import os
from datetime import datetime, timedelta

from pyspark.sql import SparkSession, SQLContext
from pyspark.sql.functions import current_timestamp, to_timestamp

from ingestor.tools import Log


_logger = Log()


class CsvLoader:

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
            target_project: str,
            target_dataset: str,
            source_bucket: str,
            source_table: str,
            target_table: str,
            temporary_gcs_bucket: str,
            materialization_dataset: str,
            target_table_schema: str,
            partition_date: str = datetime.strftime(datetime.now() - timedelta(1), '%Y-%m-%d'),
            ingestion_mode: str = 'overwrite'
    ):
        """Class responsible for reading ga shards from BigQuery and write it as a BigQuery table."""

        self._target_project = target_project
        self._target_dataset: str = target_dataset
        self._source_bucket: str = source_bucket
        self._source_table: str = source_table
        self._target_table: str = target_table
        self._target_table_schema: str = target_table_schema
        self._partition_date: str = partition_date
        self._temporary_gcs_bucket: str = temporary_gcs_bucket
        self._materialization_dataset: str = materialization_dataset
        self._ingestion_mode: str = ingestion_mode

    def load(self) -> None:
        """Read a view from BigQuery and write it as a BigQuery table."""

        _logger.info("Starting Spark application ...")

        spark = (
            SparkSession.builder.master("yarn")
            .config("spark.jars", "gs://spark-lib/bigquery/spark-bigquery-latest_2.12.jar")
            .appName("pss_spark_application")
            .getOrCreate()
        )

        spark.conf.set("viewsEnabled", "true")
        spark.conf.set("materializationDataset", self._target_dataset)
        spark.conf.set("materializationExpirationTimeInMinutes", "180")
        spark.conf.set("temporaryGcsBucket", self._temporary_gcs_bucket)

        sc = spark.sparkContext
        sql_c = SQLContext(spark.sparkContext)

        try:
            df_s = sql_c.read.csv(
                os.path.join(
                    "gs://{}".format(self._source_bucket),
                    self._source_table,
                    self._partition_date,
                    "*.DEL"
                ),
                header=False,
                sep=",",
                schema=self._target_table_schema
            )

            # def change_dtype(df):
            #     for name, dtype in df.dtypes:
            #         if dtype == "date":
            #             df = df.withColumn(name, col(name).cast('timestamp'))
            #     return df

            # df_s.createOrReplaceTempView("loading")

            df_s.createOrReplaceTempView("vw_{}_tmp".format(self._target_table))
            df = spark.sql("SELECT * FROM vw_{}_tmp".format(self._target_table))

            # count = spark.sql("SELECT count(*) as cnt FROM vw_{}_tmp".format(self._target_table)).first()["cnt"]

            _logger.debug("Reading file `{}` into a Spark Dataframe".format(
                os.path.join(
                    "gs://{}".format(self._source_bucket),
                    self._source_table,
                    self._partition_date
                    )
                )
            )

            df = df.withColumn("UPDATE_DATE", to_timestamp(current_timestamp()))
            df.schema['UPDATE_DATE'].nullable = False

            _logger.info(
                "Writing Spark DF into Bigquery table `{}.{}.{}`".format(
                    self._target_project,
                    self._target_dataset,
                    self._target_table
                )
            )

            (
                df.write.format("bigquery")
                .mode(self._ingestion_mode)
                .save("{}.{}.{}".format(self._target_project, self._target_dataset, self._target_table))
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
