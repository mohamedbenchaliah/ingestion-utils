import click

from ingestor.schema import update_schema
from ingestor.loader import upload_data_file, create_table, load_table, load_csv
from ingestor.quality import quality_scan


@click.group()
def ingestor():
    pass


ingestor.add_command(update_schema)
ingestor.add_command(upload_data_file)
ingestor.add_command(create_table)
ingestor.add_command(load_csv)
ingestor.add_command(load_table)
ingestor.add_command(quality_scan)
