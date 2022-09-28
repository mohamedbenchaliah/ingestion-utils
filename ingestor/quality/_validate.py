from typing import NamedTuple, List

from pyspark.sql import DataFrame, SparkSession

from ingestor.quality._constraints._Constraint import _Constraint     # noqa
from ingestor.quality._constraints._NotNull import _NotNull       # noqa
from ingestor.quality._constraints._Numbers import _Min, _Max, _Between       # noqa
from ingestor.quality._constraints._OneOf import _OneOf       # noqa
from ingestor.quality._constraints._Integrity import _Integrity       # noqa
from ingestor.quality._constraints._TextLength import _TextLength     # noqa
from ingestor.quality._constraints._TextRegex import _TextRegex       # noqa
from ingestor.quality._constraints._Unique import _Unique     # noqa
from ingestor.quality._constraints._StatColumn import _MeanColumn, _MedianColumn      # noqa


class ValidationError(NamedTuple):

    column_name: str
    constraint_name: str
    number_of_errors: int


class ValidationResult(NamedTuple):
    """Contains both the correct data and the data that has not passed the validation.
    In addition to the erroneous data, it also contains a list of validation errors.
    """

    correct_data: DataFrame
    erroneous_data: DataFrame
    errors: List[ValidationError]


class ValidateSparkDataFrame:
    def __init__(self, spark: SparkSession, data_frame: DataFrame):
        """ValidateSparkDataFrame."""

        self.spark: SparkSession = spark
        self.df: DataFrame = data_frame
        self.input_columns: List[str] = data_frame.columns
        self.constraints: List[_Constraint] = []

    def is_unique(self, column_name: str):
        """Defines a constraint that checks whether the given column contains only unique values."""

        self._add_constraint(_Unique(column_name))
        return self

    def are_unique(self, column_names: List[str]):
        """Defines constraints that check whether given columns contain only unique values."""

        for column_name in column_names:
            self.is_unique(column_name)
        return self

    def is_not_null(self, column_name: str):
        """Defines a constraint that does not allow null values in a given column."""

        self._add_constraint(_NotNull(column_name))
        return self

    def are_not_null(self, column_names: List[str]):
        """Defines constraints that don't allow null values in all of the given columns."""

        for column_name in column_names:
            self.is_not_null(column_name)
        return self

    def is_min(self, column_name: str, value: int):
        """Defines a constraint that check whether the given
        column contains values equal or larger than a given integer."""

        self._add_constraint(_Min(column_name, value))
        return self

    def is_max(self, column_name: str, value: int):
        """Defines a constraint that check whether the given
        column contains values equal or smaller than a given integer."""

        self._add_constraint(_Max(column_name, value))
        return self

    def is_between(self, column_name, lower_bound, upper_bound):
        """Defines a constraint that checks whether the given
        column contains a value equal to or between the lower and upper bound."""

        self._add_constraint(_Between(column_name, lower_bound, upper_bound))
        return self

    def has_length_between(self, column_name, lower_bound, upper_bound):
        """Defines a constraint that checks whether the given
        column contains a text which length is equal to or between the lower and upper bound."""

        self._add_constraint(_TextLength(column_name, lower_bound, upper_bound))
        return self

    def text_matches_regex(self, column_name, regex):
        """Defines a constraint that checks whether the content of a given column matches the given regex."""

        self._add_constraint(_TextRegex(column_name, regex))
        return self

    def one_of(self, column_name, allowed_values: list):
        """Defines a constraint that checks whether the column value is equal to one of the given values."""

        self._add_constraint(_OneOf(column_name, allowed_values))
        return self

    def one_of_integrity(self, column_name, allowed_values: list):
        """Defines a constraint that checks whether the column value is equal to one of the given values."""

        self._add_constraint(_Integrity(column_name, allowed_values))
        return self

    def mean_column_value(self, column_name: str, min_mean: float, max_mean: float):
        """Defines a constraint that checks whether the average of
        all values in the column is between the given min and max value (inclusive)."""

        self._add_constraint(_MeanColumn(column_name, min_mean, max_mean))
        return self

    def median_column_value(self, column_name: str, min_median: float, max_median: float):
        """Defines a constraint that checks whether the median of
        all values in the column is between the given min and max value (inclusive)."""

        self._add_constraint(_MedianColumn(column_name, min_median, max_median))
        return self

    def execute(self) -> ValidationResult:
        """Returns a named tuple containing the data that passed the validation,
        the data that was rejected (only unique rows), and a list of violated constraints."""

        self._validate_constraints()

        if self.constraints:
            for constraint in self.constraints:
                self.df = constraint.prepare_df_for_check(self.df)

            correct_output = self.df
            errors = []

            for constraint in self.constraints:
                correct_output = constraint.filter_success(correct_output)
                number_of_failures = constraint.filter_failure(self.df).count()

                if number_of_failures > 0:
                    errors.append(
                        ValidationError(constraint.column_name, constraint.constraint_name(), number_of_failures)
                    )

            correct_output = correct_output.select(self.input_columns)
            incorrect_output = self.df.select(self.input_columns).subtract(correct_output)

            return ValidationResult(correct_output, incorrect_output, errors)
        else:
            return ValidationResult(self.df, self.spark.createDataFrame([], self.df.schema), [])

    def _add_constraint(self, constraint: _Constraint) -> None:
        existing = filter(
            lambda c: c.constraint_name() == constraint.constraint_name() and c.column_name == constraint.column_name,
            self.constraints,
        )
        if list(existing):
            raise ValueError(f"A not_null constraint for column {constraint.column_name} already exists.")

        self.constraints.append(constraint)

    def _validate_constraints(self) -> None:
        columns = self.df.columns

        errors = []
        for constraint in self.constraints:
            is_correct, error_message = constraint.validate_self(self.df, columns)
            if not is_correct:
                errors.append(error_message)

        if errors:
            raise ValueError(", ".join(errors))
