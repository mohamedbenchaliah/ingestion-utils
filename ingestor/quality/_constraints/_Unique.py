from pyspark.sql import DataFrame

from ingestor.quality._constraints._Constraint import _Constraint


class _Unique(_Constraint):
    def __init__(self, column_name: str):
        super().__init__(column_name)

    def prepare_df_for_check(self, data_frame: DataFrame) -> DataFrame:

        # count_repetitions: DataFrame = data_frame \
        #     .groupby(self.column_name) \
        #     .count() \
        #     .withColumnRenamed("count", self.constraint_column_name)
        return data_frame

    def filter_success(self, data_frame: DataFrame) -> DataFrame:
        """Return dataframe with only all distinct values/combination
        depends on the primary key (keys if concatenate column)."""

        # return data_frame.filter(f"{self.constraint_column_name} == 1")
        return data_frame.dropDuplicates([self.column_name])

    def filter_failure(self, data_frame: DataFrame) -> DataFrame:
        """Return dataframe without unique all distinct values
        while preserving duplicates."""

        # return data_frame.filter(f"{self.constraint_column_name} > 1")
        return data_frame.exceptAll(data_frame.dropDuplicates([self.column_name]))

    def constraint_name(self):
        return "unique"
