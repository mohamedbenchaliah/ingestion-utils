from typing import Any, Callable, List, Optional, Type
import json
import copy
import zipfile

from pyspark.sql import SparkSession

try:
    from google.cloud import bigquery
except ImportError as e:
    bigquery = None
    bigquery_error = e

from ._logger import Log

_logger = Log()


def extract_zip(zip_file, dir_out):
    _logger.debug("Extracting zip file {} into {}".format(zip_file, dir_out))
    with zipfile.ZipFile(zip_file, 'r') as zip_ref:
        zip_ref.extractall(dir_out)


def transform_field(
    field,
    prefix,
    maps_from_entries=False,
    bigint_columns=None,
    transform_layer=0,
):
    """
    Generate spark SQL to recursively convert fields types.

    Notes
    -----
    If maps_from_entries is True, convert repeated key-value structs to maps.
    If bigint_columns is a list, convert non-matching BIGINT columns to INT.
    If bigint_columns is a list, convert non-matching BIGINT columns to INT.

    Parameters
    ----------
    field :str
    prefix : str
    maps_from_entries : str
    bigint_columns : List[int]
    transform_layer : int

    Retunrs
    ----------
    str
    """

    transformed = False
    result = full_name = ".".join(prefix + (field.name,))
    repeated = field.mode == "REPEATED"

    if repeated:
        if transform_layer > 0:
            prefix = ("_{}".format(transform_layer),)
        else:
            prefix = ("_",)
        transform_layer += 1
    else:
        prefix = (prefix, field.name)

    if field.field_type == "RECORD":
        if bigint_columns is not None:
            prefix_len = len(field.name) + 1
            bigint_columns = [column[prefix_len:] for column in bigint_columns if column.startswith(field.name + ".")]
        subfields = [
            transform_field(subfield, maps_from_entries, bigint_columns, transform_layer, prefix)
            for subfield in field.fields
        ]
        if any(subfield_transformed for _, subfield_transformed in subfields):
            transformed = True
            fields = ", ".join(transform for transform, _ in subfields)
            result = "STRUCT({})".format(fields)
            if repeated:
                result = "TRANSFORM({}, {} -> {})".format(full_name, prefix[0], result)
        if maps_from_entries:
            if repeated and {"key", "value"} == {f.name for f in field.fields}:
                transformed = True
                result = "MAP_FROM_ENTRIES({})".format(result)

    elif field.field_type == "INTEGER":
        if bigint_columns is not None and field.name not in bigint_columns:
            transformed = True
            if repeated:
                result = "TRANSFORM({}, {} -> INT({}))".format(full_name, prefix[0], prefix[0])
            else:
                result = "INT({})".format(full_name)

    return "{} AS {}".format(result, field.name), transformed


def transform_schema(table: str, maps_from_entries: bool = False, bigint_columns=None) -> List[str]:
    """
    Get maps_from_entries expressions for all maps in the given table.

    Parameters
    ----------
    table : str
        tables, eg. "rtc_vehicle"
    maps_from_entries : bool
        maps_from_entries .eg. True
    bigint_columns : None

    Returns
    -------
    List[str]
    """

    if bigquery is None:
        return ["..."]
    schema = sorted(bigquery.Client().get_table(table).schema, key=lambda f: f.name)
    replace = []
    for index, field in enumerate(schema):
        try:
            expr, transformed = transform_field(field, maps_from_entries, bigint_columns)
        except Exception:
            json_field = json.dumps(field.to_api_repr(), indent=2)
            print("problem with field {}:\n{}".format(index, json_field))
            raise
        if transformed:
            replace += [expr]

    return replace


def get_spark_env(
        project: str,
        temporary_gcs_bucket: str,
        materialization_dataset: str,
        expiration_time: int = 30,
        view_enabled: bool = True
):
    """Get spark session.

    Parameters
    ----------
    temporary_gcs_bucket : string
        Temporary gcs bucket name
    materialization_dataset : string
        Temporary BQ dataset needed by SaprkSQl Bigquery connector
    view_enabled : bool
        enable bigquery view
    expiration_time : int
        materialization ExpirationTimeInMinutes, e.g. 30 (min)

    Returns
    -------
    SparkSession

    Examples
    --------
    >>> from ingestor.tools import get_spark_env
    >>> session = get_spark_env( 'proj-id', 'my-artifacts-bucket', 'tmp-bq-dataset')
    """

    spark = SparkSession.builder.getOrCreate()
    spark_conf = spark.sparkContext._conf.setAll(
        [
            ("spark.sql.session.timeZone", "UTC"),
            ("spark.jars", "gs://spark-lib/bigquery/spark-bigquery-latest_2.12.jar"),
            ("temporaryGcsBucket", temporary_gcs_bucket),
            ("viewsEnabled", view_enabled),
            ("project", project),
            ("materializationProject", project),
            ("parentProject", project),
            ("materializationDataset", materialization_dataset),
            ("materializationExpirationTimeInMinutes", expiration_time),
            ("spark.sql.debug.maxToStringFields", 100000),
            ("spark.dynamicAllocation.enabled", False),
            ("spark.driver.maxResultSize", 0),
        ]
    )
    spark.sparkContext.stop()
    spark = SparkSession.builder.config(conf=spark_conf).getOrCreate()
    # spark = SparkSession.builder.master("yarn").config(conf=spark_conf).getOrCreate()

    return spark


def builder(func: Callable) -> Callable:
    """SQL query Decorator for wrapper "builder" functions."""

    def _copy(self, *args, **kwargs):
        self_copy = copy.copy(self) if getattr(self, "immutable", True) else self
        result = func(self_copy, *args, **kwargs)

        if result is None:
            return self_copy
        return result

    return _copy


def ignore_copy(func: Callable) -> Callable:
    """Decorator for wrapping the __getattr__ function for classes that are copied via deepcopy."""

    def _getattr(self, name):
        if name in [
            "__copy__",
            "__deepcopy__",
            "__getstate__",
            "__setstate__",
            "__getnewargs__",
        ]:
            raise AttributeError("'%s' object has no attribute '%s'" % (self.__class__.__name__, name))
        return func(self, name)

    return _getattr


def resolve_is_aggregate(values: List[Optional[bool]]) -> Optional[bool]:
    """Resolves the is_aggregate flag for an expression that contains multiple terms.

    Parameters
    ----------
    values : List[Optional[bool]]
        A list of booleans (or None) for each term in the expression

    Returns
    -------
    List[Optional[bool]]
    """

    result = [x for x in values if x is not None]
    if result:
        return all(result)
    return None


def format_quotes(value: Any, quote_char: Optional[str]) -> str:

    return "{quote}{value}{quote}".format(value=value, quote=quote_char or "")


def format_alias_sql(
    sql: str,
    alias: Optional[str],
    quote_char: Optional[str] = None,
    alias_quote_char: Optional[str] = None,
    as_keyword: bool = False,
    **kwargs: Any,
) -> str:
    if alias is None:
        return sql
    return "{sql}{_as}{alias}".format(
        sql=sql, _as=" AS " if as_keyword else " ", alias=format_quotes(alias, alias_quote_char or quote_char)
    )


def validate(*args: Any, exc: Optional[Exception] = None, type: Optional[Type] = None) -> None:
    if type is not None:
        for arg in args:
            if not isinstance(arg, type):
                raise BaseException(exc)
