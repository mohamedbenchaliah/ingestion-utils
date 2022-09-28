import click  # noqa

from ingestor.schema._schema_update import SchemaUpdate
from ingestor.tools import Log


_logger = Log()


@click.command()
@click.option("--target-project", "-p", "target_project", default="c4-gdw-dev")
@click.option("--target-dataset", "-d", "target_dataset", default="pss_dataset")
@click.option("--target-table", "-t", "target_table")
@click.option("--expiration-time", "-e", "expiration_time", default="2999-01-01 00:00:00 UTC")
@click.pass_context
def update_schema(ctx, **kwargs):  # noqa
    target_project = kwargs.pop("target_project")
    target_dataset = kwargs.pop("target_dataset")
    target_table = kwargs.pop("target_table")
    expiration_time = kwargs.pop("expiration_time")

    _logger.info(
        "info about task schema_update are : [{},{},{},{}]".format(
            target_project, target_dataset, target_table, expiration_time
        )
    )
    task = SchemaUpdate(target_project, target_dataset, target_table, expiration_time)
    task._update_schema()  # noqa
