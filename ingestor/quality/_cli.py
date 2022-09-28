import click  # noqa

from pyspark.sql import SparkSession, DataFrame, Row  # noqa

from ingestor.tools import Log
from ingestor.quality._quality import QualityScan  # noqa


_logger = Log()


@click.command()
@click.option("--source-project", "-p", "source_project", default="c4-gdw-dev")
@click.option("--source-dataset", "-d", "source_dataset", default="pss_dataset")
@click.option("--target-project", "-r", "target_project", default="c4-gdw-dev")
@click.option("--target-dataset", "-c", "target_dataset", default="pss_dataset")
@click.option("--target-table", "-t", "target_table", default="data_quality")
@click.option("--materialization-dataset", "-m", "materialization_dataset", default="pss_dataset_checkpoints")
@click.option("--temporary-gcs-bucket", "-g", "temporary_gcs_bucket", default="c4-gdw-pss-staging-bucket")
@click.option("--config-path", "-p", "config_path")
@click.pass_context
def quality_scan(ctx, **kwargs):  # noqa
    source_project = kwargs.pop("source_project")
    source_dataset = kwargs.pop("source_dataset")
    target_project = kwargs.pop("target_project")
    target_dataset = kwargs.pop("target_dataset")
    target_table = kwargs.pop("target_table")
    materialization_dataset = kwargs.pop("materialization_dataset")
    temporary_gcs_bucket = kwargs.pop("temporary_gcs_bucket")
    config_path = kwargs.pop("config_path")

    _logger.info(
        "info about task are : [{}, {}, {}, {}, {}, {}, {}, {}]".format(
            source_project,
            source_dataset,
            target_project,
            target_dataset,
            target_table,
            materialization_dataset,
            temporary_gcs_bucket,
            config_path,
        )
    )
    task = QualityScan(
        source_project,
        source_dataset,
        target_project,
        target_dataset,
        target_table,
        materialization_dataset,
        temporary_gcs_bucket,
        config_path,
    )
    task.run_quality_check()
