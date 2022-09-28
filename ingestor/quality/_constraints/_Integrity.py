from functools import reduce
from operator import and_
from typing import List

from pyspark.sql import DataFrame
from pyspark.sql.functions import col, coalesce, lit

from ingestor.quality._constraints._Constraint import _Constraint


class _Integrity(_Constraint):
    def __init__(self, column_name: str, allowed_values: list):
        super().__init__(column_name)
        self.allowed_values = allowed_values

    def prepare_df_for_check(self, data_frame: DataFrame) -> DataFrame:
        return data_frame

    def is_identical(self, x, y, keys: List[str]):
        def both_null(c):
            return col("x.{}".format(c)).isNull() & col("y.{}".format(c)).isNull()  # noqa

        def both_equal(c):
            return coalesce((col("x.{}".format(c)) == col("y.{}".format(c))), lit(False))

        p = reduce(and_, [both_null(c) | both_equal(c) for c in x.columns])
        return x.alias("x").join(y.alias("y"), keys, "full_outer").where(~p).count()

    def filter_success(self, data_frame: DataFrame) -> DataFrame:
        return data_frame.filter(data_frame[self.column_name].isin(*self.allowed_values))

    def filter_failure(self, data_frame: DataFrame) -> DataFrame:
        return data_frame.filter(~data_frame[self.column_name].isin(*self.allowed_values))

    def constraint_name(self):
        return "integrity"
