import json

from pyspark.sql.functions import concat, to_timestamp, current_timestamp, lit, when, col, lpad
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, FloatType

from ingestor.tools import Log, get_spark_env
from ingestor.quality._validate import ValidateSparkDataFrame
from ingestor._exceptions import BigQueryTablesNotFound


_logger = Log()


class QualityScan:

    _filter_null_default = False

    def __init__(
        self,
        source_project: str,
        source_dataset: str,
        target_project: str,
        target_dataset: str,
        target_table: str,
        materialization_dataset: str,
        temporary_gcs_bucket: str,
        config_path: str,
    ):
        """scan a BigQuery table and write result into a BigQuery data_quality table."""

        self._source_project: str = source_project if source_project else ""
        self._source_dataset: str = source_dataset
        self._target_project: str = target_project if target_project else ""
        self._target_dataset: str = target_dataset if target_dataset else source_dataset
        self._target_table: str = target_table
        self._temporary_gcs_bucket: str = temporary_gcs_bucket
        self._materialization_dataset: str = materialization_dataset
        self._config_path: str = config_path

    @staticmethod
    def _get_type(df, col_name):
        return [dtype for name, dtype in df.dtypes if name == col_name][0]

    def _check_integrity(self, x, y, keys):
        if y.count() == 0:
            return 0
        return x.alias("x").join(y.alias("y"), keys, "leftsemi").count()

    def run_quality_check(self) -> None:
        """Runs BigQuery check and write it into a BigQuery table."""

        _logger.info("Starting Spark application ...")

        spark = get_spark_env(
            project=self._source_project,
            temporary_gcs_bucket=self._temporary_gcs_bucket,
            materialization_dataset=self._materialization_dataset,
            expiration_time=1440,  # expiration after 24 hours
            view_enabled=True,
        )
        sc = spark.sparkContext

        _logger.info(f"Loading json config from path `{self._config_path}` ...")

        with open(f"{self._config_path}") as config_file:
            conf_data = json.load(config_file)

        if set(["table", "checks", "cols_to_load"]).issubset(conf_data.keys()):  # noqa
            table = conf_data["table"]
            checks = conf_data["checks"]
            cols_to_load = conf_data["cols_to_load"]
        else:
            _logger.error(
                "Missing keys in the input config file. "
                "Please Make sure `domain` `layer` `stage` `table` and `checks` are present. "
            )
            raise

        if "additional_args" in conf_data.keys():
            additional_args = conf_data["additional_args"]
        else:
            additional_args = ""

        try:
            col_split = conf_data["col_split"]
            _logger.info(f"Aggregation column: {col_split}")
        except Exception as e:
            col_split = ""
            list_split = ["none"]
            _logger.info("No aggregation column defined. Set to `none`. ")

        query_text = "select "
        for c in cols_to_load:
            query_text += f" {c}, "
        if len(col_split) > 0 and col_split not in cols_to_load:
            query_text += f" {col_split}, "
        query_text += f" '{table}' as table_name ".replace("project_id", self._source_project).replace(
            "dataset_id", self._source_dataset
        )
        query_text += f" FROM `{table}`".replace("project_id", self._source_project).replace(
            "dataset_id", self._source_dataset
        )
        query_text += f" {additional_args} ;"

        _df = (spark.read.format("bigquery").load(query_text)).cache()

        try:
            col_split = conf_data["col_split"]
            list_split = [
                list(x.asDict().values())[0]
                for x in _df.select(col_split).distinct().collect()
                if x and len(col_split) > 0
            ]
            list_split = [*{*list_split}]
        except Exception as e:
            col_split = ""
            list_split = ["none"]
            _logger.info("No split column defined for aggregation. ")

        schema = StructType(
            [
                StructField("column_name", StringType(), True),
                StructField("constraint_name", StringType(), True),
                StructField("number_errors", IntegerType(), True),
                StructField("number_rows", IntegerType(), True),
                StructField("percent_coherence", FloatType(), True),
                StructField("split_key", StringType(), True),
                StructField("split_value", StringType(), True),
            ]
        )
        final_df = spark.createDataFrame(spark.sparkContext.emptyRDD(), schema)

        for elem in list_split:
            list_checks = []

            if len(col_split) > 1:
                if elem is None or len(elem) == 0:
                    elem = "UNKNOWN"
                    df = _df.where(col(col_split).isNull())
                else:
                    df = _df.where(_df[col_split] == elem)
            else:
                col_split = "none"
                df = _df

            for check in checks:

                if check["check_type"] == "regex_match":

                    for current_column in list(set(check["args"]["columns"])):
                        _df_check_regex = df

                        if "filter_null" in check["args"].keys() and check["args"]["filter_null"]:
                            _df_check_regex = df.filter(col(current_column).isNotNull())
                        cd_cnt = _df_check_regex.count()

                        _df_check_regex = _df_check_regex.withColumn(
                                f"{current_column}", _df_check_regex[f"{current_column}"].cast(StringType())
                            )
                        validity = (
                            ValidateSparkDataFrame(spark, _df_check_regex)
                            .text_matches_regex(current_column, check["args"]["regex"])
                            .execute()
                        )
                        _df_check_regex.unpersist()

                        if len(validity.errors) > 0 and cd_cnt > 0:
                            for val_err in validity.errors:
                                column_name, constraint_name, number_of_errors = val_err
                                __data = {
                                    "column_name": column_name,
                                    "constraint_name": constraint_name,
                                    "number_rows": cd_cnt,
                                    "number_errors": number_of_errors,
                                    "split_key": col_split,
                                    "split_value": elem,
                                    "percent_coherence": round(100 - 100 * (number_of_errors / cd_cnt), 4),
                                }
                                list_checks.append(__data)
                        elif cd_cnt == 0:
                            __data = {
                                "column_name": current_column,
                                "constraint_name": "regex_match",
                                "number_rows": cd_cnt,
                                "number_errors": 0,
                                "split_key": col_split,
                                "split_value": elem,
                                "percent_coherence": 0,
                            }
                            list_checks.append(__data)
                        else:
                            __data = {
                                "column_name": current_column,
                                "constraint_name": "regex_match",
                                "number_rows": cd_cnt,
                                "number_errors": 0,
                                "split_key": col_split,
                                "split_value": elem,
                                "percent_coherence": 100,
                            }
                            list_checks.append(__data)

                if check["check_type"] == "not_null":
                    for current_column in list(set(check["args"]["columns"])):
                        _df_check_not_null = df
                        validity = (
                            ValidateSparkDataFrame(spark, _df_check_not_null).is_not_null(current_column).execute()
                        )
                        cd_cnt = _df_check_not_null.count()
                        _df_check_not_null.unpersist()

                        if len(validity.errors) > 0 and cd_cnt > 0:
                            for val_err in validity.errors:
                                column_name, constraint_name, number_of_errors = val_err
                                __data = {
                                    "column_name": column_name,
                                    "constraint_name": constraint_name,
                                    "number_rows": cd_cnt,
                                    "number_errors": number_of_errors,
                                    "split_key": col_split,
                                    "split_value": elem,
                                    "percent_coherence": round(100 - 100 * (number_of_errors / cd_cnt), 4),
                                }
                                list_checks.append(__data)
                        elif cd_cnt == 0:
                            __data = {
                                "column_name": current_column,
                                "constraint_name": "not_null",
                                "number_rows": cd_cnt,
                                "number_errors": 0,
                                "split_key": col_split,
                                "split_value": elem,
                                "percent_coherence": 0,
                            }
                            list_checks.append(__data)
                        else:
                            __data = {
                                "column_name": current_column,
                                "constraint_name": "not_null",
                                "number_rows": cd_cnt,
                                "number_errors": 0,
                                "split_key": col_split,
                                "split_value": elem,
                                "percent_coherence": 100,
                            }
                            list_checks.append(__data)

                if check["check_type"] == "unique":
                    _df_check_unique = df
                    _df_check_unique = _df_check_unique.withColumn(
                        "cols_to_join_pk", concat(*list(set(check["args"]["columns"])))
                    )

                    # Check if filter_null keys exist and is true,
                    # Then filter all null value in current checked column
                    if "filter_null" in check["args"].keys() and check["args"]["filter_null"]:
                        _df_check_unique = _df_check_unique.filter(col("cols_to_join_pk").isNotNull())
                    cd_cnt = _df_check_unique.count()
                    validity = ValidateSparkDataFrame(spark, _df_check_unique).is_unique("cols_to_join_pk").execute()
                    _df_check_unique.unpersist()

                    if len(validity.errors) > 0 and cd_cnt > 0:
                        for val_err in validity.errors:
                            column_name, constraint_name, number_of_errors = val_err
                            data = {
                                "column_name": " ".join(list(set(check["args"]["columns"]))),
                                "constraint_name": constraint_name,
                                "number_rows": cd_cnt,
                                "number_errors": number_of_errors,
                                "split_key": col_split,
                                "split_value": elem,
                                "percent_coherence": round(100 - 100 * (number_of_errors / cd_cnt), 4),
                            }
                            list_checks.append(data)
                    elif cd_cnt == 0:
                        data = {
                            "column_name": " ".join(list(set(check["args"]["columns"]))),
                            "constraint_name": "unique",
                            "number_rows": cd_cnt,
                            "number_errors": 0,
                            "split_key": col_split,
                            "split_value": elem,
                            "percent_coherence": 0,
                        }
                        list_checks.append(data)
                    else:
                        data = {
                            "column_name": " ".join(list(set(check["args"]["columns"]))),
                            "constraint_name": "unique",
                            "number_rows": cd_cnt,
                            "number_errors": 0,
                            "split_key": col_split,
                            "split_value": elem,
                            "percent_coherence": 100,
                        }
                        list_checks.append(data)

                if check["check_type"] == "one_of":
                    for current_column in list(set(check["args"]["columns"])):
                        _df_check_one_of = df

                        # Check if filter_null keys exist and is true,
                        # Then filter all null value in current checked column
                        if "filter_null" in check["args"].keys() and check["args"]["filter_null"]:
                            _df_check_one_of = _df_check_one_of.filter(col(current_column).isNotNull())

                        cd_cnt = _df_check_one_of.count()
                        validity = (
                            ValidateSparkDataFrame(spark, _df_check_one_of)
                            .one_of(current_column, check["args"]["allowed_values"])
                            .execute()
                        )
                        _df_check_one_of.unpersist()

                        if len(validity.errors) > 0 and cd_cnt > 0:
                            for val_err in validity.errors:
                                column_name, constraint_name, number_of_errors = val_err
                                data = {
                                    "column_name": column_name,
                                    "constraint_name": constraint_name,
                                    "number_rows": cd_cnt,
                                    "number_errors": number_of_errors,
                                    "split_key": col_split,
                                    "split_value": elem,
                                    "percent_coherence": round(100 - 100 * (number_of_errors / cd_cnt), 4),
                                }
                                list_checks.append(data)
                        elif cd_cnt == 0:
                            data = {
                                "column_name": current_column,
                                "constraint_name": "one_of",
                                "number_rows": cd_cnt,
                                "number_errors": 0,
                                "split_key": col_split,
                                "split_value": elem,
                                "percent_coherence": 0,
                            }
                            list_checks.append(data)
                        else:
                            data = {
                                "column_name": current_column,
                                "constraint_name": "one_of",
                                "number_rows": cd_cnt,
                                "number_errors": 0,
                                "split_key": col_split,
                                "split_value": elem,
                                "percent_coherence": 100,
                            }
                            list_checks.append(data)

                if check["check_type"] == "between":
                    for current_column in list(set(check["args"]["columns"])):
                        _df_check_between = df

                        # Check if filter_null keys exist and is true,
                        # Then filter all null value in current checked column
                        if "filter_null" in check["args"].keys() and check["args"]["filter_null"]:
                            _df_check_between = _df_check_between.filter(col(current_column).isNotNull())

                        cd_cnt = _df_check_between.count()
                        validity = (
                            ValidateSparkDataFrame(spark, _df_check_between)
                            .is_between(current_column, check["args"]["min_val"], check["args"]["max_val"])
                            .execute()
                        )
                        _df_check_between.unpersist()

                        if len(validity.errors) > 0 and cd_cnt > 0:
                            for val_err in validity.errors:
                                column_name, constraint_name, number_of_errors = val_err
                                data = {
                                    "column_name": column_name,
                                    "constraint_name": constraint_name,
                                    "number_rows": cd_cnt,
                                    "number_errors": number_of_errors,
                                    "split_key": col_split,
                                    "split_value": elem,
                                    "percent_coherence": round(100 - 100 * (number_of_errors / cd_cnt), 4),
                                }
                                list_checks.append(data)
                        elif cd_cnt == 0:
                            data = {
                                "column_name": current_column,
                                "constraint_name": "between",
                                "number_rows": cd_cnt,
                                "number_errors": 0,
                                "split_key": col_split,
                                "split_value": elem,
                                "percent_coherence": 0,
                            }
                            list_checks.append(data)
                        else:
                            data = {
                                "column_name": current_column,
                                "constraint_name": "between",
                                "number_rows": cd_cnt,
                                "number_errors": 0,
                                "split_key": col_split,
                                "split_value": elem,
                                "percent_coherence": 100,
                            }
                            list_checks.append(data)

                if check["check_type"] == "text_length":
                    for current_column in list(set(check["args"]["columns"])):
                        _df_check_length = df

                        # Check if filter_null keys exist and is true,
                        # Then filter all null value in current checked column
                        if "filter_null" in check["args"].keys() and check["args"]["filter_null"]:
                            _df_check_length = _df_check_length.filter(col(current_column).isNotNull())

                        cd_cnt = _df_check_length.count()
                        validity = (
                            ValidateSparkDataFrame(spark, _df_check_length)
                            .has_length_between(
                                current_column, check["args"]["lower_bound"], check["args"]["upper_bound"]
                            )
                            .execute()
                        )
                        _df_check_length.unpersist()

                        if len(validity.errors) > 0 and cd_cnt > 0:
                            for val_err in validity.errors:
                                column_name, constraint_name, number_of_errors = val_err
                                data = {
                                    "column_name": column_name,
                                    "constraint_name": constraint_name,
                                    "number_rows": cd_cnt,
                                    "number_errors": number_of_errors,
                                    "split_key": col_split,
                                    "split_value": elem,
                                    "percent_coherence": round(100 - 100 * (number_of_errors / cd_cnt), 4),
                                }
                                list_checks.append(data)
                        elif cd_cnt == 0:
                            data = {
                                "column_name": current_column,
                                "constraint_name": "text_length",
                                "number_rows": cd_cnt,
                                "number_errors": 0,
                                "split_key": col_split,
                                "split_value": elem,
                                "percent_coherence": 0,
                            }
                            list_checks.append(data)
                        else:
                            data = {
                                "column_name": current_column,
                                "constraint_name": "text_length",
                                "number_rows": cd_cnt,
                                "number_errors": 0,
                                "split_key": col_split,
                                "split_value": elem,
                                "percent_coherence": 100,
                            }
                            list_checks.append(data)

                if check["check_type"] == "min":
                    for current_column in list(set(check["args"]["columns"])):
                        _df_check_min = df
                        # Check if filter_null keys exist and is true,
                        # Then filter all null value in current checked column
                        if "filter_null" in check["args"].keys() and check["args"]["filter_null"]:
                            _df_check_min = _df_check_min.filter(col(current_column).isNotNull())

                        cd_cnt = _df_check_min.count()
                        validity = (
                            ValidateSparkDataFrame(spark, _df_check_min)
                            .is_min(current_column, check["args"]["min_value"])
                            .execute()
                        )

                        _df_check_min.unpersist()

                        if len(validity.errors) > 0 and cd_cnt > 0:
                            for val_err in validity.errors:
                                column_name, constraint_name, number_of_errors = val_err
                                data = {
                                    "column_name": column_name,
                                    "constraint_name": constraint_name,
                                    "number_rows": cd_cnt,
                                    "number_errors": number_of_errors,
                                    "split_key": col_split,
                                    "split_value": elem,
                                    "percent_coherence": round(100 - 100 * (number_of_errors / cd_cnt), 4),
                                }
                                list_checks.append(data)
                        elif cd_cnt == 0:
                            data = {
                                "column_name": current_column,
                                "constraint_name": "min",
                                "number_rows": cd_cnt,
                                "number_errors": 0,
                                "split_key": col_split,
                                "split_value": elem,
                                "percent_coherence": 0,
                            }
                            list_checks.append(data)
                        else:
                            data = {
                                "column_name": current_column,
                                "constraint_name": "min",
                                "number_rows": cd_cnt,
                                "number_errors": 0,
                                "split_key": col_split,
                                "split_value": elem,
                                "percent_coherence": 100,
                            }
                            list_checks.append(data)

                if check["check_type"] == "max":
                    for current_column in list(set(check["args"]["columns"])):
                        _df_check_max = df
                        # Check if filter_null keys exist and is true,
                        # Then filter all null value in current checked column
                        if "filter_null" in check["args"].keys() and check["args"]["filter_null"]:
                            _df_check_max = _df_check_max.filter(col(current_column).isNotNull())

                        cd_cnt = _df_check_max.count()
                        validity = (
                            ValidateSparkDataFrame(spark, _df_check_max)
                            .is_min(current_column, check["args"]["max_value"])
                            .execute()
                        )

                        _df_check_max.unpersist()

                        if len(validity.errors) > 0 and cd_cnt > 0:
                            for val_err in validity.errors:
                                column_name, constraint_name, number_of_errors = val_err
                                data = {
                                    "column_name": column_name,
                                    "constraint_name": constraint_name,
                                    "number_rows": cd_cnt,
                                    "number_errors": number_of_errors,
                                    "split_key": col_split,
                                    "split_value": elem,
                                    "percent_coherence": round(100 - 100 * (number_of_errors / cd_cnt), 4),
                                }
                                list_checks.append(data)
                        elif cd_cnt == 0:
                            data = {
                                "column_name": current_column,
                                "constraint_name": "max",
                                "number_rows": cd_cnt,
                                "number_errors": 0,
                                "split_key": col_split,
                                "split_value": elem,
                                "percent_coherence": 0,
                            }
                            list_checks.append(data)

                        else:
                            data = {
                                "column_name": current_column,
                                "constraint_name": "max",
                                "number_rows": cd_cnt,
                                "number_errors": 0,
                                "split_key": col_split,
                                "split_value": elem,
                                "percent_coherence": 100,
                            }
                            list_checks.append(data)

                if check["check_type"] == "mean_between":
                    for current_column in list(set(check["args"]["columns"])):
                        _df_check_mean_between = df
                        # Check if filter_null keys exist and is true,
                        # Then filter all null value in current checked column
                        if "filter_null" in check["args"].keys() and check["args"]["filter_null"]:
                            _df_check_mean_between = _df_check_mean_between.filter(col(current_column).isNotNull())

                        cd_cnt = _df_check_mean_between.count()
                        validity = (
                            ValidateSparkDataFrame(spark, _df_check_mean_between)
                            .mean_column_value(
                                current_column, check["args"]["lower_bound_float"], check["args"]["upper_bound_float"]
                            )
                            .execute()
                        )

                        _df_check_mean_between.unpersist()

                        if len(validity.errors) > 0 and cd_cnt > 0:
                            for val_err in validity.errors:
                                column_name, constraint_name, number_of_errors = val_err
                                data = {
                                    "column_name": column_name,
                                    "constraint_name": constraint_name,
                                    "number_rows": cd_cnt,
                                    "number_errors": number_of_errors,
                                    "split_key": col_split,
                                    "split_value": elem,
                                    "percent_coherence": round(100 - 100 * (number_of_errors / cd_cnt), 4),
                                }
                                list_checks.append(data)
                        elif cd_cnt == 0:
                            data = {
                                "column_name": current_column,
                                "constraint_name": "mean_between",
                                "number_rows": cd_cnt,
                                "number_errors": 0,
                                "split_key": col_split,
                                "split_value": elem,
                                "percent_coherence": 0,
                            }
                            list_checks.append(data)
                        else:
                            data = {
                                "column_name": current_column,
                                "constraint_name": "mean_between",
                                "number_rows": cd_cnt,
                                "number_errors": 0,
                                "split_key": col_split,
                                "split_value": elem,
                                "percent_coherence": 100,
                            }
                            list_checks.append(data)

                if check["check_type"] == "median_between":
                    for current_column in list(set(check["args"]["columns"])):
                        _df_check_median_between = df
                        # Check if filter_null keys exist and is true,
                        # Then filter all null value in current checked column
                        if "filter_null" in check["args"].keys() and check["args"]["filter_null"]:
                            _df_check_median_between = _df_check_median_between.filter(col(current_column).isNotNull())

                        cd_cnt = _df_check_median_between.count()
                        validity = (
                            ValidateSparkDataFrame(spark, _df_check_median_between)
                            .median_column_value(
                                current_column, check["args"]["lower_bound_float"], check["args"]["upper_bound_float"]
                            )
                            .execute()
                        )

                        _df_check_median_between.unpersist()

                        if len(validity.errors) > 0 and cd_cnt > 0:
                            for val_err in validity.errors:
                                column_name, constraint_name, number_of_errors = val_err
                                data = {
                                    "column_name": column_name,
                                    "constraint_name": constraint_name,
                                    "number_rows": cd_cnt,
                                    "number_errors": number_of_errors,
                                    "split_key": col_split,
                                    "split_value": elem,
                                    "percent_coherence": round(100 - 100 * (number_of_errors / cd_cnt), 4),
                                }
                                list_checks.append(data)
                        elif cd_cnt == 0:
                            data = {
                                "column_name": current_column,
                                "constraint_name": "median_between",
                                "number_rows": cd_cnt,
                                "number_errors": 0,
                                "split_key": col_split,
                                "split_value": elem,
                                "percent_coherence": 0,
                            }
                            list_checks.append(data)
                        else:
                            data = {
                                "column_name": current_column,
                                "constraint_name": "median_between",
                                "number_rows": cd_cnt,
                                "number_errors": 0,
                                "split_key": col_split,
                                "split_value": elem,
                                "percent_coherence": 100,
                            }
                            list_checks.append(data)

                if check["check_type"].__contains__("consistency"):
                    for current_column in list(set(check["args"]["columns"])):
                        _df_check_one_of = df

                        # Check if filter_null keys exist and is true,
                        # Then filter all null value in current checked column
                        if "filter_null" in check["args"].keys() and check["args"]["filter_null"]:
                            _df_check_one_of = _df_check_one_of.filter(col(current_column).isNotNull())

                        _ch = check["check_type"].split("_")[-1].upper()
                        _df_check_one_of = _df_check_one_of.filter(
                            _df_check_one_of[f"{check['args']['Extra_filter_col']}"] == _ch
                        ).withColumn("check_col", concat(lit(_ch), _df_check_one_of.vin.substr(1, 3)))

                        _df_check_one_of = _df_check_one_of.select("check_col")

                        cd_cnt = _df_check_one_of.count()
                        validity = (
                            ValidateSparkDataFrame(spark, _df_check_one_of)
                            .one_of("check_col", check["args"]["allowed_values"])
                            .execute()
                        )
                        _df_check_one_of.unpersist()

                        if len(validity.errors) > 0 and cd_cnt > 0:
                            for val_err in validity.errors:
                                column_name, constraint_name, number_of_errors = val_err
                                data = {
                                    "column_name": check["args"]["columns"][0],
                                    "constraint_name": check["check_type"],
                                    "number_rows": cd_cnt,
                                    "number_errors": number_of_errors,
                                    "split_key": col_split,
                                    "split_value": elem,
                                    "percent_coherence": round(100 - 100 * (number_of_errors / cd_cnt), 4),
                                }
                                list_checks.append(data)
                        elif cd_cnt == 0:
                            data = {
                                "column_name": check["args"]["columns"][0],
                                "constraint_name": check["check_type"],
                                "number_rows": cd_cnt,
                                "number_errors": 0,
                                "split_key": col_split,
                                "split_value": elem,
                                "percent_coherence": 0,
                            }
                            list_checks.append(data)
                        else:
                            data = {
                                "column_name": check["args"]["columns"][0],
                                "constraint_name": check["check_type"],
                                "number_rows": cd_cnt,
                                "number_errors": 0,
                                "split_key": col_split,
                                "split_value": elem,
                                "percent_coherence": 100,
                            }
                            list_checks.append(data)

            rdd_result = sc.parallelize([list_checks])
            df2 = spark.read.option("mode", "DROPMALFORMED").json(rdd_result)
            final_df = final_df.union(df2)

        final_df = (
            final_df.withColumn("table_name", lit(f"{table}".split(".")[-1]))
            .withColumn("scan_time", to_timestamp(current_timestamp()))
            .withColumn(
                "kpi_name",
                when(final_df.constraint_name == "regex_match", "validity")
                .when(final_df.constraint_name == "one_of", "validity")
                .when(final_df.constraint_name.contains("consistency"), "consistency")
                .when(final_df.constraint_name == "between", "validity")
                .when(final_df.constraint_name == "unique", "uniqueness")
                .when(final_df.constraint_name == "not_null", "completeness")
                .when(final_df.constraint_name == "median_between", "validity")
                .when(final_df.constraint_name == "mean_between", "validity")
                .when(final_df.constraint_name == "max", "validity")
                .when(final_df.constraint_name == "min", "validity")
                .when(final_df.constraint_name == "text_length", "validity")
                .otherwise("other"),
            )
        )

        try:
            (
                final_df.write.format("bigquery")
                .mode("append")
                .save("{}.{}.{}".format(self._target_project, self._target_dataset, self._target_table))
            )

        except BigQueryTablesNotFound as e:
            _logger.error(
                "Unexpected error while writing to Bigquery table `{}.{}.{}`. ".format(
                    self._target_project, self._target_dataset, self._target_table
                )
            )
            raise e
