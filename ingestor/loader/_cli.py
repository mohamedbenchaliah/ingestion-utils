import click  # noqa
from datetime import datetime, timedelta, date

from ingestor.tools import Log
from ingestor.loader._create import Bootstrapper  # noqa
from ingestor.loader._table_loader import TableLoader  # noqa
from ingestor.loader._file_data_loader import FileDataLoader  # noqa
from ingestor.loader._csv_reader import CsvLoader  # noqa


_logger = Log()


@click.command()
@click.option("--file", "-f", "file")
@click.option("--target-project", "-p", "target_project", default="c4-gdw-dev")
@click.option("--expiration-time", "-e", "expiration_time", default="2999-01-01 00:00:00 UTC")
@click.option("--table-prefix", "-t", "table_prefix", default="")
@click.pass_context
def create_table(ctx, **kwargs):  # noqa
    file = kwargs.pop("file")
    target_project = kwargs.pop("target_project")
    expiration_time = kwargs.pop("expiration_time")
    table_prefix = kwargs.pop("table_prefix")

    _logger.info("info about task are : [{}, {}, {}, {}]".format(file, target_project, expiration_time, table_prefix))
    Bootstrapper.bootstrap(file, target_project, expiration_time, table_prefix)


@click.command()
@click.option("--target-project", "-p", "target_project", default="c4-gdw-dev")
@click.option("--target-dataset", "-d", "target_dataset", default="pss_dataset")
@click.option("--target-table", "-t", "target_table")
@click.option("--file", "-f", "file_path", help="data file to load to BQ.")
@click.option("--schema", "-s", "schema_path", help="schema file to apply to BQ table.")
@click.pass_context
def upload_data_file(ctx, **kwargs):  # noqa
    target_project = kwargs.pop("target_project")
    target_dataset = kwargs.pop("target_dataset")
    target_table = kwargs.pop("target_table")
    file_path = kwargs.pop("file_path")
    schema_path = kwargs.pop("schema_path")

    _logger.info(
        "info about task are : [{}, {}, {}, {}, {}]".format(
            target_project, target_dataset, target_table, file_path, schema_path
        )
    )
    task = FileDataLoader(target_project, target_dataset, target_table, file_path, schema_path)
    task.load_file_data()


@click.command()
@click.option("--source-project", "source_project")
@click.option("--source-dataset", "source_dataset", default="products_referential")
@click.option("--target-project", "target_project")
@click.option("--target-dataset", "target_dataset", default="products_referential")
@click.option("--target-table", "target_table")
@click.option("--file", "-f", "file")
@click.option("--temporary-gcs-bucket", "temporary_gcs_bucket")
@click.option("--materialization-dataset", "materialization_dataset")
@click.pass_context
def load_table(ctx, **kwargs):  # noqa
    source_project = kwargs.pop("source_project")
    source_dataset = kwargs.pop("source_dataset")
    target_project = kwargs.pop("target_project")
    target_dataset = kwargs.pop("target_dataset")
    target_table = kwargs.pop("target_table")
    file = kwargs.pop("file")
    temporary_gcs_bucket = kwargs.pop("temporary_gcs_bucket")
    materialization_dataset = kwargs.pop("materialization_dataset")

    _logger.info(
        "info about task are : [{}, {}, {}, {}, {}, {}, {}, {}]".format(
            source_project,
            source_dataset,
            target_project,
            target_dataset,
            target_table,
            file,
            temporary_gcs_bucket,
            materialization_dataset
        )
    )
    task = TableLoader(
        source_project,
        source_dataset,
        target_project,
        target_dataset,
        target_table,
        file,
        temporary_gcs_bucket,
        materialization_dataset
    )
    task.load()


@click.command()
@click.option("--target-project", "target_project")
@click.option("--target-dataset", "target_dataset")
@click.option("--source-bucket", "source_bucket")
@click.option("--source-table", "source_table")
@click.option("--target-table", "target_table")
@click.option("--temporary-gcs-bucket", "temporary_gcs_bucket")     # noqa
@click.option("--materialization-dataset", "materialization_dataset")
@click.option("--target-table-schema", "target_table_schema")
@click.option(
    "--partition-date",
    "partition_date",
    default=datetime.strftime(date.today() - timedelta(days=date.today().weekday()), '%Y/%m/%d')
)
@click.option("--ingestion-mode", "ingestion_mode", default="overwrite")
@click.pass_context
def load_csv(ctx, **kwargs):  # noqa
    target_project = kwargs.pop("target_project")
    target_dataset = kwargs.pop("target_dataset")
    target_table = kwargs.pop("target_table")
    temporary_gcs_bucket = kwargs.pop("temporary_gcs_bucket")
    materialization_dataset = kwargs.pop("materialization_dataset")
    source_bucket = kwargs.pop("source_bucket")
    source_table = kwargs.pop("source_table")
    partition_date = kwargs.pop("partition_date")
    target_table_schema = kwargs.pop("target_table_schema")
    ingestion_mode = kwargs.pop("ingestion_mode")

    _logger.info(
        "info about task are : [{},{}, {}, {}, {}, {}, {}, {}, {}, {}]".format(
            target_project,
            target_dataset,
            target_table,
            temporary_gcs_bucket,
            materialization_dataset,
            partition_date,
            source_bucket,
            source_table,
            target_table_schema,
            ingestion_mode
        )
    )

    task = CsvLoader(
        target_project,
        target_dataset,
        source_bucket,
        source_table,
        target_table,
        temporary_gcs_bucket,
        materialization_dataset,
        target_table_schema,
        partition_date,
        ingestion_mode
    )
    task.load()
