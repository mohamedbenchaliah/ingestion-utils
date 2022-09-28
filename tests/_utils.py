from typing import Optional, List, Union

from pandas import DataFrame as Pandas_df
from numpy.testing import assert_array_equal
from pyspark.sql import DataFrame as Spark_df
from pyspark.sql import DataFrame
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType

from smh_engine.checker.validate_df import ValidationResult, ValidationError


single_string_column_schema = StructType([StructField("col1", StringType())])
two_string_columns_schema = StructType([StructField("col1", StringType()), StructField("col2", StringType())])

single_integer_column_schema = StructType([StructField("col1", IntegerType())])
two_integer_columns_schema = StructType([StructField("col1", IntegerType()), StructField("col2", IntegerType())])


def empty_string_df(spark_session: SparkSession):
    return spark_session.createDataFrame([], schema=single_string_column_schema)


def empty_integer_df(spark_session: SparkSession):
    return spark_session.createDataFrame([], schema=single_integer_column_schema)


class AssertDf:
    def __init__(self, df: Spark_df, order_by_column: Optional[Union[str, List[str]]] = None):
        self.df: Pandas_df = df.toPandas()
        self.order_by_column = order_by_column

    def is_empty(self):
        assert self.df.empty
        return self

    def contains_exactly(self, other: Pandas_df):
        if self.order_by_column:
            sorted_df = self.df.sort_values(self.order_by_column)
            other_sorted = other.sort_values(self.order_by_column)
            assert_array_equal(sorted_df.values, other_sorted.values, verbose=True)
        else:
            assert self.df.equals(other)
        return self

    def has_columns(self, columns: list):
        existing_columns = sorted(list(self.df.columns))
        expected_columns = sorted(columns)
        assert existing_columns == expected_columns, f"{existing_columns} != {expected_columns}"
        return self

    def has_n_rows(self, n):
        assert self.df.shape[0] == n
        return self


class AssertValidationResult:
    def __init__(self, *, column_name: str, constraint_name: str):
        self.column_name = column_name
        self.constraint_name = constraint_name

    def check(
            self,
            *,
            actual: ValidationResult,
            expected_correct: DataFrame,
            expected_erroneous: DataFrame
    ):
        if expected_correct.count() == 0:
            AssertDf(actual.correct_data) \
                .is_empty() \
                .has_columns(expected_correct.columns)
        else:
            AssertDf(actual.correct_data, order_by_column=self.column_name) \
                .contains_exactly(expected_correct.toPandas()) \
                .has_columns(expected_correct.columns)

        if expected_erroneous.count() == 0:
            AssertDf(actual.erroneous_data) \
                .is_empty() \
                .has_columns(expected_erroneous.columns)
        else:
            AssertDf(actual.erroneous_data, order_by_column=self.column_name) \
                .contains_exactly(expected_erroneous.toPandas()) \
                .has_columns(expected_erroneous.columns)

        if expected_erroneous.count() == 0:
            assert actual.errors == []
        else:
            assert actual.errors == [
                ValidationError(
                    self.column_name,
                    self.constraint_name,
                    expected_erroneous.count()
                )
            ]
