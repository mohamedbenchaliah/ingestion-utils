from google.cloud import bigquery


def str_to_list_tuples(s):
    return eval("%s" % s)


def add_argument(parser, *args, **kwargs):
    """Add default to help while adding argument to parser."""

    if "help" in kwargs:
        default = kwargs.get("default")
        if default not in (None, [], [None]):
            if kwargs.get("nargs") in ("*", "+"):
                try:
                    (default,) = default
                except ValueError:
                    pass
            kwargs["help"] += "; Defaults to {}".format(default)
    parser.add_argument(*args, **kwargs)


def add_parallelism(parser, default=4):
    """Add argument for parallel execution."""

    add_argument(
        parser,
        "--parallelism",
        default=default,
        type=int,
        help="Maximum number of tasks to execute concurrently",
    )


def add_source_project(parser):
    """Add argument for source project."""

    add_argument(
        parser, "--source-project",
        type=str,
        dest="source_project",
        default="gdw_utility",
        help="Source Project"
    )


def add_source_dataset(parser):
    """Add argument for source dataset."""

    add_argument(
        parser,
        "--source-dataset",
        type=str,
        dest="source_dataset",
        help="The name of the source dataset",
        default="gdw_utility",
    )


def add_source_table(parser):
    """Add argument for source dataset."""

    add_argument(
        parser,
        "--source-table",
        type=str,
        dest="source_table",
        help="The name of the source table to process"
    )


def add_target_project(parser):
    """Add argument for target project."""

    add_argument(
        parser,
        "--target-project",
        type=str,
        dest="target_project",
        default="c4-gdw-dev",
        help="Target Project"
    )


def add_target_dataset(parser):
    """Add argument for target dataset."""

    add_argument(
        parser,
        "--target-dataset",
        type=str,
        dest="target_dataset",
        default="gdw_utility",
        help="Target Dataset"
    )


def add_target_table(parser):
    """Add argument for target table."""

    add_argument(
        parser,
        "--target-table",
        type=str,
        dest="target_table",
        help="The name of the destination table. Defaults to the source table name."
        " If this ends with a version by matching /_v[0-9]+$/ it will be converted to"
        " a directory in the output.",
        default="test",
    )


def add_compare_clusters(parser):
    """Add argument for compare table clusters."""

    add_argument(
        parser,
        "--compare-clusters",
        type=str,
        dest="compare_clusters",
        help="compare table clusters."
    )


def add_compare_partition_max(parser):
    """Add argument for compare partition max date."""

    add_argument(
        parser,
        "--compare-partition-max",
        type=str,
        dest="compare_partition_max",
        help="compare partition max date."
    )


def add_compare_partition_min(parser):
    """Add argument for compare partition min date."""

    add_argument(
        parser, "--compare-partition-min", type=str, dest="compare_partition_min", help="compare partition min date."
    )


def add_compare_partition_field(parser):
    """Add argument for compare partition field."""

    add_argument(
        parser, "--compare-partition-field", type=str, dest="compare_partition_field", help="compare partition field."
    )


def add_base_clusters(parser):
    """Add argument for base table clusters."""

    add_argument(parser, "--base-clusters", type=str, dest="base_clusters", help="base table clusters.")


def add_base_partition_max(parser):
    """Add argument for base partition max date."""

    add_argument(parser, "--base-partition-max", type=str, dest="base_partition_max", help="base partition max date.")


def add_base_partition_min(parser):
    """Add argument for base partition min date."""

    add_argument(parser, "--base-partition-min", type=str, dest="base_partition_min", help="base partition min date.")


def add_base_partition_field(parser):
    """Add argument for base partition field."""

    add_argument(parser, "--base-partition-field", type=str, dest="base_partition_field", help="base partition field.")


def add_more_filters(parser):
    """Add argument for additional filters to apply to DFs."""

    add_argument(
        parser, "--filters", type=str_to_list_tuples, dest="filters", help="additional filters to apply to DFs."
    )


def add_mapping_columns(parser):
    """Add argument for mapping columns between 2 DFs."""

    add_argument(
        parser,
        "--mapping-columns",
        type=str_to_list_tuples,
        dest="mapping_columns",
        help="mapping columns between 2 DFs.",
    )


def add_join_columns(parser):
    """Add argument for joining columns between 2 DFs."""

    add_argument(
        parser, "--join-columns", type=str_to_list_tuples, dest="join_columns", help="joining columns between 2 DFs."
    )


def add_base_table_full_id(parser):
    """Add argument for full base table id to be compared with."""

    add_argument(parser, "--base-table-id", type=str, dest="base_table_id", help="The full table id of the base table.")


def add_compare_table_full_id(parser):
    """Add argument for full compare table id to be compared to."""

    add_argument(
        parser, "--compare-table-id", type=str, dest="compare_table_id", help="The full table id of the base table."
    )


def add_target_table_full_id(parser):
    """Add argument for full compare table id to be compared to."""

    add_argument(
        parser,
        "--target-table-id",
        type=str,
        dest="target_table_id",
        help="The full table id where the tables comparison will written into.",
    )


def add_table_prefix(parser, default=""):
    """Add argument for table prefix."""

    add_argument(
        parser,
        "--table-prefix",
        "--table_prefix",
        type=str,
        dest="table_prefix",
        default=default,
        help="table prefix used to switch versions, e.g. tmp, ... ",
    )


def add_temp_gcs(parser):
    """Add argument for temporary gcs bucket."""

    add_argument(
        parser,
        "--temporary-gcs-bucket",
        type=str,
        dest="temporary_gcs_bucket",
        help="tmp GCS Bucket",
        default="c4-gdw-pss-staging-bucket",
    )


def add_materialization_dataset(parser):
    """Add argument for materialization dataset."""

    add_argument(
        parser,
        "--materialization-dataset",
        type=str,
        dest="materialization_dataset",
        help="BQ materialization dataset",
        default="gdw_utility",
    )


def add_where_clause(parser):
    """Add argument for additional argument clause."""

    add_argument(
        parser,
        "--additional-arguments",
        type=str,
        dest="additional_arguments",
        help="additional where clause arguments",
    )


def add_expiration_time(parser):
    """Add argument for tables expiration time."""

    add_argument(
        parser,
        "--expiration-time",
        "--expiration_time",
        dest="expiration_time",
        default="2999-01-01 00:00:00 UTC",
        type=str,
        help="Set a table expiration time, usefully for integration tests. ",
    )


def add_sql_path(parser):
    """Add argument for tables expiration time."""

    add_argument(parser, "--file", dest="file", help="sql file to process.")


def add_priority(parser):
    """Add argument for BigQuery job priority."""

    add_argument(
        parser,
        "--priority",
        default=bigquery.QueryPriority.INTERACTIVE,
        type=str.upper,
        choices=[bigquery.QueryPriority.BATCH, bigquery.QueryPriority.INTERACTIVE],
        help="Priority for BigQuery query jobs; BATCH priority may significantly slow "
        "down queries if reserved slots are not enabled for the billing project; "
        "INTERACTIVE priority is limited to 100 concurrent queries per project",
    )
