"""BigQuery Utils Module."""

from ingestor.schema._schema_update import SchemaUpdate  # noqa
from ingestor.schema._cli import update_schema  # noqa

__all__ = ["SchemaUpdate", "update_schema"]
