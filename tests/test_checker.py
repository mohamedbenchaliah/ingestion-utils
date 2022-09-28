import pytest

from tests._utils import (
    empty_integer_df,
    empty_string_df,
    single_integer_column_schema,
    two_integer_columns_schema,
    single_string_column_schema,
    two_string_columns_schema
)
from tests._utils import AssertValidationResult
from tests._utils import AssertDf
from ingestor.quality._validate import ValidateSparkDataFrame, ValidationError


def test_should_return_df_without_changes_if_empty_df_with_is_between_constraint_between(spark_session):
    df = empty_integer_df(spark_session)
    result = ValidateSparkDataFrame(spark_session, df) \
        .is_between("col1", 5, 10) \
        .execute()
    AssertValidationResult(column_name="col1", constraint_name="between") \
        .check(
        actual=result,
        expected_correct=df,
        expected_erroneous=df
    )


def test_should_return_df_without_changes_if_all_are_between(spark_session):
    df = spark_session.createDataFrame([[5], [10], [15]], schema=single_integer_column_schema)
    result = ValidateSparkDataFrame(spark_session, df) \
        .is_between("col1", 5, 15) \
        .execute()
    AssertValidationResult(column_name="col1", constraint_name="between") \
        .check(
        actual=result,
        expected_correct=df,
        expected_erroneous=empty_integer_df(spark_session)
    )


def test_should_reject_all_rows_if_not_between(spark_session):
    df = spark_session.createDataFrame(
        [[5], [10], [20]],
        schema=single_integer_column_schema
    )
    result = ValidateSparkDataFrame(spark_session, df) \
        .is_between("col1", 11, 19) \
        .execute()
    AssertValidationResult(column_name="col1", constraint_name="between") \
        .check(
        actual=result,
        expected_correct=empty_integer_df(spark_session),
        expected_erroneous=df
    )


def test_should_return_both_correct_and_incorrect_rows_between(spark_session):
    df = spark_session.createDataFrame(
        [[5], [10], [15]],
        schema=single_integer_column_schema
    )
    expected_correct = spark_session.createDataFrame([[5], [10]], schema=single_integer_column_schema)
    expected_errors = spark_session.createDataFrame([[15]], schema=single_integer_column_schema)
    result = ValidateSparkDataFrame(spark_session, df) \
        .is_between("col1", 0, 10) \
        .execute()
    AssertValidationResult(column_name="col1", constraint_name="between") \
        .check(
        actual=result,
        expected_correct=expected_correct,
        expected_erroneous=expected_errors
    )


def test_between_ignores_the_other_column_between(spark_session):
    df = spark_session.createDataFrame([[5, 8], [10, 20], [15, 8]], schema=two_integer_columns_schema)
    expected_correct = spark_session.createDataFrame([[5, 8], [10, 20]], schema=two_integer_columns_schema)
    expected_errors = spark_session.createDataFrame([[15, 8]], schema=two_integer_columns_schema)
    result = ValidateSparkDataFrame(spark_session, df) \
        .is_between("col1", 5, 10) \
        .execute()
    AssertDf(result.correct_data, order_by_column="col1") \
        .contains_exactly(expected_correct.toPandas()) \
        .has_columns(["col1", "col2"])
    AssertDf(result.erroneous_data, order_by_column="col2") \
        .contains_exactly(expected_errors.toPandas()) \
        .has_columns(["col1", "col2"])
    assert result.errors == [ValidationError("col1", "between", 1)]


def test_between_should_check_all_given_columns_separately_between(spark_session):
    df = spark_session.createDataFrame([[25, 1], [30, 2], [35, 3]], schema=two_integer_columns_schema)
    expected_correct = spark_session.createDataFrame([], schema=two_integer_columns_schema)
    expected_errors = spark_session.createDataFrame([[25, 1], [30, 2], [35, 3]], schema=two_integer_columns_schema)
    result = ValidateSparkDataFrame(spark_session, df) \
        .is_between("col1", 0, 5) \
        .is_between("col2", 20, 40) \
        .execute()
    AssertDf(result.correct_data, order_by_column="col1") \
        .contains_exactly(expected_correct.toPandas()) \
        .has_columns(["col1", "col2"])
    AssertDf(result.erroneous_data, order_by_column="col2") \
        .contains_exactly(expected_errors.toPandas()) \
        .has_columns(["col1", "col2"])
    assert result.errors == [ValidationError("col1", "between", 3), ValidationError("col2", "between", 3)]


def test_should_throw_error_if_constraint_is_not_a_numeric_column_between(spark_session):
    with pytest.raises(ValueError):
        ValidateSparkDataFrame(spark_session, empty_string_df(spark_session)) \
            .is_between("col1", 5, 10) \
            .execute()


def test_should_throw_error_if_constraint_uses_non_existing_column_between(spark_session):
    with pytest.raises(ValueError):
        ValidateSparkDataFrame(spark_session, empty_integer_df(spark_session)) \
            .is_between("column_that_does_not_exist", 5, 5) \
            .execute()


def test_should_throw_error_if_there_are_duplicate_constraints_between(spark_session):
    with pytest.raises(ValueError):
        ValidateSparkDataFrame(spark_session, empty_integer_df(spark_session)) \
            .is_between("col1", 5, 10) \
            .is_between("col1", 5, 15) \
            .execute()


def test_should_throw_error_if_lower_bound_is_greater_than_upper_bound_between(spark_session):
    with pytest.raises(ValueError):
        ValidateSparkDataFrame(spark_session, empty_integer_df(spark_session)) \
            .is_between("col1", 10, 5) \
            .execute()


def test_should_return_df_without_changes_if_empty_df_with_is_max_constraint_max(spark_session):
    df = empty_integer_df(spark_session)
    result = ValidateSparkDataFrame(spark_session, df) \
        .is_max("col1", 5) \
        .execute()
    AssertValidationResult(column_name="col1", constraint_name="max") \
        .check(
        actual=result,
        expected_correct=df,
        expected_erroneous=df
    )


def test_should_return_df_without_changes_if_all_rows_smaller_than_max(spark_session):
    df = spark_session.createDataFrame([[5], [10], [15]], schema=single_integer_column_schema)
    result = ValidateSparkDataFrame(spark_session, df) \
        .is_max("col1", 20) \
        .execute()
    AssertValidationResult(column_name="col1", constraint_name="max") \
        .check(
        actual=result,
        expected_correct=df,
        expected_erroneous=empty_integer_df(spark_session)
    )


def test_should_reject_all_rows_if_larger_than_max(spark_session):
    df = spark_session.createDataFrame([[5], [10], [15]], schema=single_integer_column_schema)
    result = ValidateSparkDataFrame(spark_session, df) \
        .is_max("col1", 1) \
        .execute()
    AssertValidationResult(column_name="col1", constraint_name="max") \
        .check(
        actual=result,
        expected_correct=empty_integer_df(spark_session),
        expected_erroneous=df
    )


def test_should_return_both_correct_and_incorrect_rows_max(spark_session):
    df = spark_session.createDataFrame([[5], [10], [15]], schema=single_integer_column_schema)
    expected_correct = spark_session.createDataFrame([[5], [10]], schema=single_integer_column_schema)
    expected_errors = spark_session.createDataFrame([[15]], schema=single_integer_column_schema)
    result = ValidateSparkDataFrame(spark_session, df) \
        .is_max("col1", 10) \
        .execute()
    AssertValidationResult(column_name="col1", constraint_name="max") \
        .check(
        actual=result,
        expected_correct=expected_correct,
        expected_erroneous=expected_errors
    )


def test_max_value_of_other_columns_is_ignored_max(spark_session):
    df = spark_session.createDataFrame([[5, 1], [10, 20], [15, 1]], schema=two_integer_columns_schema)
    expected_correct = spark_session.createDataFrame([[5, 1], [10, 20]], schema=two_integer_columns_schema)
    expected_errors = spark_session.createDataFrame([[15, 1]], schema=two_integer_columns_schema)
    result = ValidateSparkDataFrame(spark_session, df) \
        .is_max("col1", 10) \
        .execute()
    AssertValidationResult(column_name="col1", constraint_name="max") \
        .check(
        actual=result,
        expected_correct=expected_correct,
        expected_erroneous=expected_errors
    )


def test_max_should_check_all_given_columns_separately_max(spark_session):
    df = spark_session.createDataFrame([[25, 1], [30, 2], [35, 3]], schema=two_integer_columns_schema)
    expected_correct = spark_session.createDataFrame([], schema=two_integer_columns_schema)
    expected_errors = spark_session.createDataFrame([[25, 1], [30, 2], [35, 3]], schema=two_integer_columns_schema)
    result = ValidateSparkDataFrame(spark_session, df) \
        .is_max("col1", 20) \
        .is_max("col2", 0) \
        .execute()
    AssertDf(result.correct_data, order_by_column="col1") \
        .contains_exactly(expected_correct.toPandas()) \
        .has_columns(["col1", "col2"])
    AssertDf(result.erroneous_data, order_by_column="col2") \
        .contains_exactly(expected_errors.toPandas()) \
        .has_columns(["col1", "col2"])
    assert result.errors == [ValidationError("col1", "max", 3), ValidationError("col2", "max", 3)]


def test_should_throw_error_if_constraint_is_not_a_numeric_column_max(spark_session):
    with pytest.raises(ValueError):
        ValidateSparkDataFrame(spark_session, empty_string_df(spark_session)) \
            .is_max("col1", 5) \
            .execute()


def test_should_throw_error_if_constraint_uses_non_existing_column_max(spark_session):
    with pytest.raises(ValueError):
        ValidateSparkDataFrame(spark_session, empty_integer_df(spark_session)) \
            .is_max("column_that_does_not_exist", 5) \
            .execute()


def test_should_throw_error_if_there_are_duplicate_constraints_max(spark_session):
    with pytest.raises(ValueError):
        ValidateSparkDataFrame(spark_session, empty_integer_df(spark_session)) \
            .is_max("col1", 5) \
            .is_max("col1", 10) \
            .execute()


def test_should_return_df_without_changes_if_empty_df_with_mean_constraint_mean(spark_session):
    df = empty_integer_df(spark_session)
    result = ValidateSparkDataFrame(spark_session, df) \
        .mean_column_value("col1", 0, 1) \
        .execute()
    AssertValidationResult(column_name="col1", constraint_name="mean_between") \
        .check(
        actual=result,
        expected_correct=df,
        expected_erroneous=df
    )


def test_should_return_df_without_changes_if_the_mean_is_between_given_values_mean(spark_session):
    df = spark_session.createDataFrame([[5], [10], [15]], schema=single_integer_column_schema)
    result = ValidateSparkDataFrame(spark_session, df) \
        .mean_column_value("col1", 5, 15) \
        .execute()
    AssertValidationResult(column_name="col1", constraint_name="mean_between") \
        .check(
        actual=result,
        expected_correct=df,
        expected_erroneous=empty_integer_df(spark_session)
    )


def test_should_reject_all_rows_if_mean_is_smaller_than_given_values_mean(spark_session):
    df = spark_session.createDataFrame([[5], [10], [15]], schema=single_integer_column_schema)
    result = ValidateSparkDataFrame(spark_session, df) \
        .mean_column_value("col1", 12, 15) \
        .execute()
    AssertValidationResult(column_name="col1", constraint_name="mean_between") \
        .check(
        actual=result,
        expected_correct=empty_integer_df(spark_session),
        expected_erroneous=df
    )


def test_should_reject_all_rows_if_mean_is_larger_than_given_values_mean(spark_session):
    df = spark_session.createDataFrame([[5], [10], [15]], schema=single_integer_column_schema)
    result = ValidateSparkDataFrame(spark_session, df) \
        .mean_column_value("col1", 5, 8) \
        .execute()
    AssertValidationResult(column_name="col1", constraint_name="mean_between") \
        .check(
        actual=result,
        expected_correct=empty_integer_df(spark_session),
        expected_erroneous=df
    )


def test_mean_value_of_other_columns_is_ignored_mean(spark_session):
    df = spark_session.createDataFrame([[5, 1], [10, 2], [15, 3]], schema=two_integer_columns_schema)
    expected_errors = spark_session.createDataFrame([], schema=two_integer_columns_schema)
    result = ValidateSparkDataFrame(spark_session, df) \
        .mean_column_value("col1", 10, 10) \
        .execute()
    AssertValidationResult(column_name="col1", constraint_name="mean_between") \
        .check(
        actual=result,
        expected_correct=df,
        expected_erroneous=expected_errors
    )


def test_mean_should_check_all_given_columns_separately_mean(spark_session):
    df = spark_session.createDataFrame([[5, 1], [10, 2], [15, 3]], schema=two_integer_columns_schema)
    expected_errors = spark_session.createDataFrame([], schema=two_integer_columns_schema)
    result = ValidateSparkDataFrame(spark_session, df) \
        .mean_column_value("col1", 10, 10) \
        .mean_column_value("col2", 2, 2) \
        .execute()
    AssertValidationResult(column_name="col1", constraint_name="mean_between") \
        .check(
        actual=result,
        expected_correct=df,
        expected_erroneous=expected_errors
    )


def test_should_throw_error_if_constraint_is_not_a_numeric_column_mean(spark_session):
    with pytest.raises(ValueError):
        ValidateSparkDataFrame(spark_session, empty_string_df(spark_session)) \
            .mean_column_value("col1", 10, 10) \
            .execute()


def test_should_throw_error_if_constraint_uses_non_existing_column_mean(spark_session):
    with pytest.raises(ValueError):
        ValidateSparkDataFrame(spark_session, empty_integer_df(spark_session)) \
            .mean_column_value("column_that_does_not_exist", 5, 5) \
            .execute()


def test_should_throw_error_if_there_are_duplicate_constraints_mean(spark_session):
    with pytest.raises(ValueError):
        ValidateSparkDataFrame(spark_session, empty_integer_df(spark_session)) \
            .mean_column_value("col1", 10, 10) \
            .mean_column_value("col1", 5, 5) \
            .execute()


def test_should_return_df_without_changes_if_empty_df_with_median_constraint(spark_session):
    df = empty_integer_df(spark_session)
    result = ValidateSparkDataFrame(spark_session, df) \
        .median_column_value("col1", 0, 1) \
        .execute()
    AssertValidationResult(column_name="col1", constraint_name="median_between") \
        .check(
        actual=result,
        expected_correct=df,
        expected_erroneous=df
    )


def test_should_return_df_without_changes_if_the_median_is_between_given_values(spark_session):
    df = spark_session.createDataFrame([[5], [10], [15]], schema=single_integer_column_schema)
    result = ValidateSparkDataFrame(spark_session, df) \
        .median_column_value("col1", 5, 15) \
        .execute()
    AssertValidationResult(column_name="col1", constraint_name="median_between") \
        .check(
        actual=result,
        expected_correct=df,
        expected_erroneous=empty_integer_df(spark_session)
    )


def test_should_reject_all_rows_if_median_is_smaller_than_given_values(spark_session):
    df = spark_session.createDataFrame([[5], [10], [15]], schema=single_integer_column_schema)
    result = ValidateSparkDataFrame(spark_session, df) \
        .median_column_value("col1", 12, 15) \
        .execute()
    AssertValidationResult(column_name="col1", constraint_name="median_between") \
        .check(
        actual=result,
        expected_correct=empty_integer_df(spark_session),
        expected_erroneous=df
    )


def test_should_reject_all_rows_if_median_is_larger_than_given_values(spark_session):
    df = spark_session.createDataFrame([[5], [10], [15]], schema=single_integer_column_schema)
    result = ValidateSparkDataFrame(spark_session, df) \
        .median_column_value("col1", 5, 8) \
        .execute()
    AssertValidationResult(column_name="col1", constraint_name="median_between") \
        .check(
        actual=result,
        expected_correct=empty_integer_df(spark_session),
        expected_erroneous=df
    )


def test_median_value_of_other_columns_is_ignored(spark_session):
    df = spark_session.createDataFrame([[5, 1], [10, 2], [15, 3]], schema=two_integer_columns_schema)
    expected_errors = spark_session.createDataFrame([], schema=two_integer_columns_schema)
    result = ValidateSparkDataFrame(spark_session, df) \
        .median_column_value("col1", 10, 10) \
        .execute()
    AssertValidationResult(column_name="col1", constraint_name="median_between") \
        .check(
        actual=result,
        expected_correct=df,
        expected_erroneous=expected_errors
    )


def test_median_should_check_all_given_columns_separately(spark_session):
    df = spark_session.createDataFrame([[5, 1], [10, 2], [15, 3]], schema=two_integer_columns_schema)
    expected_errors = spark_session.createDataFrame([], schema=two_integer_columns_schema)
    result = ValidateSparkDataFrame(spark_session, df) \
        .median_column_value("col1", 10, 10) \
        .median_column_value("col2", 2, 2) \
        .execute()
    AssertValidationResult(column_name="col1", constraint_name="median_between") \
        .check(
        actual=result,
        expected_correct=df,
        expected_erroneous=expected_errors
    )


def test_should_throw_error_if_constraint_is_not_a_numeric_column_median(spark_session):
    with pytest.raises(ValueError):
        ValidateSparkDataFrame(spark_session, empty_string_df(spark_session)) \
            .median_column_value("col1", 10, 10) \
            .execute()


def test_should_throw_error_if_constraint_uses_non_existing_column_median(spark_session):
    with pytest.raises(ValueError):
        ValidateSparkDataFrame(spark_session, empty_integer_df(spark_session)) \
            .median_column_value("column_that_does_not_exist", 5, 5) \
            .execute()


def test_should_throw_error_if_there_are_duplicate_constraints_median(spark_session):
    with pytest.raises(ValueError):
        ValidateSparkDataFrame(spark_session, empty_integer_df(spark_session)) \
            .median_column_value("col1", 10, 10) \
            .median_column_value("col1", 5, 5) \
            .execute()


def test_should_return_df_without_changes_if_empty_df_with_is_min_constraint(spark_session):
    df = empty_integer_df(spark_session)
    result = ValidateSparkDataFrame(spark_session, df) \
        .is_min("col1", 5) \
        .execute()
    AssertValidationResult(column_name="col1", constraint_name="min") \
        .check(
        actual=result,
        expected_correct=df,
        expected_erroneous=df
    )


def test_should_return_df_without_changes_if_all_rows_greater_than_min(spark_session):
    df = spark_session.createDataFrame([[5], [10], [15]], schema=single_integer_column_schema)
    result = ValidateSparkDataFrame(spark_session, df) \
        .is_min("col1", 5) \
        .execute()
    AssertValidationResult(column_name="col1", constraint_name="min") \
        .check(
        actual=result,
        expected_correct=df,
        expected_erroneous=empty_integer_df(spark_session)
    )


def test_should_reject_all_rows_if_smaller_than_min(spark_session):
    df = spark_session.createDataFrame([[5], [10], [15]], schema=single_integer_column_schema)
    result = ValidateSparkDataFrame(spark_session, df) \
        .is_min("col1", 20) \
        .execute()
    AssertValidationResult(column_name="col1", constraint_name="min") \
        .check(
        actual=result,
        expected_correct=empty_integer_df(spark_session),
        expected_erroneous=df
    )


def test_should_return_both_correct_and_incorrect_rows_min(spark_session):
    df = spark_session.createDataFrame([[5], [10], [15]], schema=single_integer_column_schema)
    expected_correct = spark_session.createDataFrame([[10], [15]], schema=single_integer_column_schema)
    expected_errors = spark_session.createDataFrame([[5]], schema=single_integer_column_schema)
    result = ValidateSparkDataFrame(spark_session, df) \
        .is_min("col1", 10) \
        .execute()
    AssertValidationResult(column_name="col1", constraint_name="min") \
        .check(
        actual=result,
        expected_correct=expected_correct,
        expected_erroneous=expected_errors
    )


def test_min_value_of_other_columns_is_ignored(spark_session):
    df = spark_session.createDataFrame([[5, 1], [10, 2], [15, 3]], schema=two_integer_columns_schema)
    expected_correct = spark_session.createDataFrame([[10, 2], [15, 3]], schema=two_integer_columns_schema)
    expected_errors = spark_session.createDataFrame([[5, 1]], schema=two_integer_columns_schema)
    result = ValidateSparkDataFrame(spark_session, df) \
        .is_min("col1", 10) \
        .execute()
    AssertValidationResult(column_name="col1", constraint_name="min") \
        .check(
        actual=result,
        expected_correct=expected_correct,
        expected_erroneous=expected_errors
    )


def test_min_should_check_all_given_columns_separately(spark_session):
    df = spark_session.createDataFrame([[5, 1], [10, 2], [15, 3]], schema=two_integer_columns_schema)
    expected_correct = spark_session.createDataFrame([], schema=two_integer_columns_schema)
    expected_errors = spark_session.createDataFrame([[5, 1], [10, 2], [15, 3]], schema=two_integer_columns_schema)
    result = ValidateSparkDataFrame(spark_session, df) \
        .is_min("col1", 20) \
        .is_min("col2", 5) \
        .execute()
    AssertDf(result.correct_data, order_by_column="col1") \
        .contains_exactly(expected_correct.toPandas()) \
        .has_columns(["col1", "col2"])
    AssertDf(result.erroneous_data, order_by_column="col2") \
        .contains_exactly(expected_errors.toPandas()) \
        .has_columns(["col1", "col2"])

    assert result.errors == [ValidationError("col1", "min", 3), ValidationError("col2", "min", 3)]


def test_should_throw_error_if_constraint_is_not_a_numeric_column_min(spark_session):
    with pytest.raises(ValueError):
        ValidateSparkDataFrame(spark_session, empty_string_df(spark_session)) \
            .is_min("col1", 5) \
            .execute()


def test_should_throw_error_if_constraint_uses_non_existing_column_min(spark_session):
    with pytest.raises(ValueError):
        ValidateSparkDataFrame(spark_session, empty_integer_df(spark_session)) \
            .is_min("column_that_does_not_exist", 5) \
            .execute()


def test_should_throw_error_if_there_are_duplicate_constraints_min(spark_session):
    with pytest.raises(ValueError):
        ValidateSparkDataFrame(spark_session, empty_integer_df(spark_session)) \
            .is_min("col1", 5) \
            .is_min("col1", 10) \
            .execute()


def test_should_return_rows_that_pass_all_checks_and_reject_rows_that_violate_any_test(spark_session):
    not_between = [25, 1]
    max_exceeded = [3, 30]
    correct = [3, 15]
    less_than_min = [1, 15]
    both_wrong = [7, 30]
    df = spark_session.createDataFrame(
        [not_between, max_exceeded, correct, less_than_min, both_wrong],
        schema=two_integer_columns_schema
    )
    expected_correct = spark_session.createDataFrame([correct], schema=two_integer_columns_schema)
    expected_errors = spark_session.createDataFrame(
        [not_between, max_exceeded, less_than_min, both_wrong],
        schema=two_integer_columns_schema
    )
    result = ValidateSparkDataFrame(spark_session, df) \
        .is_between("col1", 0, 5) \
        .is_min("col1", 3) \
        .is_max("col2", 20) \
        .execute()
    AssertDf(result.correct_data, order_by_column="col1") \
        .contains_exactly(expected_correct.toPandas()) \
        .has_columns(["col1", "col2"])
    AssertDf(result.erroneous_data, order_by_column="col2") \
        .contains_exactly(expected_errors.toPandas()) \
        .has_columns(["col1", "col2"])
    assert result.errors == [
        ValidationError("col1", "between", 2),
        ValidationError("col1", "min", 1),
        ValidationError("col2", "max", 2)
    ]


def test_should_pass_empty_df_with_not_null_constraint(spark_session):
    df = empty_string_df(spark_session)
    result = ValidateSparkDataFrame(spark_session, df) \
        .is_not_null("col1") \
        .execute()
    AssertValidationResult(column_name="col1", constraint_name="not_null") \
        .check(
        actual=result,
        expected_correct=df,
        expected_erroneous=df
    )


def test_should_return_df_without_changes_if_all_rows_are_not_null(spark_session):
    df = spark_session.createDataFrame([["abc"], ["def"], ["ghi"]], schema=single_string_column_schema)
    result = ValidateSparkDataFrame(spark_session, df) \
        .is_not_null("col1") \
        .execute()
    AssertValidationResult(column_name="col1", constraint_name="not_null") \
        .check(
        actual=result,
        expected_correct=df,
        expected_erroneous=empty_string_df(spark_session)
    )


def test_should_reject_all_rows_if_all_are_null(spark_session):
    df = spark_session.createDataFrame([[None], [None], [None]], schema=single_string_column_schema)
    expected_errors = spark_session.createDataFrame([[None]], schema=single_string_column_schema)
    result = ValidateSparkDataFrame(spark_session, df) \
        .is_not_null("col1") \
        .execute()
    AssertDf(result.correct_data) \
        .is_empty() \
        .has_columns(["col1"])
    AssertDf(result.erroneous_data) \
        .contains_exactly(expected_errors.toPandas()) \
        .has_columns(["col1"])
    assert result.errors == [ValidationError("col1", "not_null", 3)]


def test_should_return_both_correct_and_incorrect_rows(spark_session):
    df = spark_session.createDataFrame([["abc"], [None]], schema=single_string_column_schema)
    expected_correct = spark_session.createDataFrame([["abc"]], schema=single_string_column_schema)
    expected_errors = spark_session.createDataFrame([[None]], schema=single_string_column_schema)
    result = ValidateSparkDataFrame(spark_session, df) \
        .is_not_null("col1") \
        .execute()
    AssertValidationResult(column_name="col1", constraint_name="not_null") \
        .check(
        actual=result,
        expected_correct=expected_correct,
        expected_erroneous=expected_errors
    )


def test_nulls_in_other_columns_are_ignored(spark_session):
    df = spark_session.createDataFrame([["abc", "123"], [None, "456"], ["def", None]], schema=two_string_columns_schema)
    expected_correct = spark_session.createDataFrame([["abc", "123"], ["def", None]], schema=two_string_columns_schema)
    expected_errors = spark_session.createDataFrame([[None, "456"]], schema=two_string_columns_schema)
    result = ValidateSparkDataFrame(spark_session, df) \
        .is_not_null("col1") \
        .execute()
    AssertValidationResult(column_name="col1", constraint_name="not_null") \
        .check(
        actual=result,
        expected_correct=expected_correct,
        expected_erroneous=expected_errors
    )


def test_not_null_should_check_all_given_columns_separately(spark_session):
    df = spark_session.createDataFrame(
        [["abc", None], [None, "456"], [None, None]],
        schema=two_string_columns_schema
    )
    expected_errors = spark_session.createDataFrame(
        [["abc", None], [None, "456"], [None, None]],
        schema=two_string_columns_schema
    )
    result = ValidateSparkDataFrame(spark_session, df) \
        .is_not_null("col1") \
        .is_not_null("col2") \
        .execute()
    AssertDf(result.correct_data) \
        .is_empty() \
        .has_columns(["col1", "col2"])
    AssertDf(result.erroneous_data, order_by_column=["col1", "col2"]) \
        .contains_exactly(expected_errors.toPandas()) \
        .has_columns(["col1", "col2"])
    assert result.errors == [ValidationError("col1", "not_null", 2), ValidationError("col2", "not_null", 2)]


def test_not_null_should_check_all_given_columns_separately_even_if_all_of_them_are_defined_at_once(spark_session):
    df = spark_session.createDataFrame(
        [["abc", None], [None, "456"], [None, None]],
        schema=two_string_columns_schema
    )
    expected_errors = spark_session.createDataFrame(
        [["abc", None], [None, "456"], [None, None]],
        schema=two_string_columns_schema
    )
    result = ValidateSparkDataFrame(spark_session, df) \
        .are_not_null(["col1", "col2"]) \
        .execute()
    AssertDf(result.correct_data) \
        .is_empty() \
        .has_columns(["col1", "col2"])
    AssertDf(result.erroneous_data, order_by_column=["col1", "col2"]) \
        .contains_exactly(expected_errors.toPandas()) \
        .has_columns(["col1", "col2"])
    assert result.errors == [ValidationError("col1", "not_null", 2), ValidationError("col2", "not_null", 2)]


def test_should_throw_error_if_constraint_uses_non_existing_column(spark_session):
    with pytest.raises(ValueError):
        ValidateSparkDataFrame(spark_session, empty_string_df(spark_session)) \
            .is_not_null("column_that_does_not_exist") \
            .execute()


def test_should_throw_error_if_there_are_duplicate_constraints(spark_session):
    with pytest.raises(ValueError):
        ValidateSparkDataFrame(spark_session, empty_string_df(spark_session)) \
            .is_not_null("col1") \
            .is_not_null("col1") \
            .execute()


def test_should_return_df_without_changes_if_empty_df_with_one_of_constraint(spark_session):
    df = empty_string_df(spark_session)
    result = ValidateSparkDataFrame(spark_session, df) \
        .one_of("col1", []) \
        .execute()
    AssertValidationResult(column_name="col1", constraint_name="one_of") \
        .check(
        actual=result,
        expected_correct=df,
        expected_erroneous=df
    )


def test_should_return_df_without_changes_if_all_are_in_list(spark_session):
    df = spark_session.createDataFrame([["abc"], ["def"], ["ghi"]], schema=single_string_column_schema)
    result = ValidateSparkDataFrame(spark_session, df) \
        .one_of("col1", ["abc", "def", "ghi"]) \
        .execute()
    AssertValidationResult(column_name="col1", constraint_name="one_of") \
        .check(
        actual=result,
        expected_correct=df,
        expected_erroneous=empty_string_df(spark_session)
    )


def test_should_reject_all_rows_if_none_of_them_is_in_the_list(spark_session):
    df = spark_session.createDataFrame(
        [["abc"], ["a"], ["abcdefghi"]],
        schema=single_string_column_schema
    )
    expected_errors = spark_session.createDataFrame(
        [["abc"], ["a"], ["abcdefghi"]],
        schema=single_string_column_schema
    )
    result = ValidateSparkDataFrame(spark_session, df) \
        .one_of("col1", ["ab", "b"]) \
        .execute()
    AssertValidationResult(column_name="col1", constraint_name="one_of") \
        .check(
        actual=result,
        expected_correct=empty_string_df(spark_session),
        expected_erroneous=expected_errors
    )


def test_should_return_both_correct_and_incorrect_rows_one_of(spark_session):
    df = spark_session.createDataFrame(
        [["a"], ["abc"], ["defg"], ["hijkl"]],
        schema=single_string_column_schema
    )
    expected_correct = spark_session.createDataFrame([["abc"], ["defg"]], schema=single_string_column_schema)
    expected_errors = spark_session.createDataFrame([["a"], ["hijkl"]], schema=single_string_column_schema)
    result = ValidateSparkDataFrame(spark_session, df) \
        .one_of("col1", ["abc", "defg"]) \
        .execute()
    AssertValidationResult(column_name="col1", constraint_name="one_of") \
        .check(
        actual=result,
        expected_correct=expected_correct,
        expected_erroneous=expected_errors
    )


def test_should_return_both_correct_and_incorrect_rows_numeric_values(spark_session):
    df = spark_session.createDataFrame([[1], [2], [3], [4]], schema=single_string_column_schema)
    expected_correct = spark_session.createDataFrame([[1], [3]], schema=single_string_column_schema)
    expected_errors = spark_session.createDataFrame([[2], [4]], schema=single_string_column_schema)
    result = ValidateSparkDataFrame(spark_session, df) \
        .one_of("col1", [1, 3, 5]) \
        .execute()
    AssertValidationResult(column_name="col1", constraint_name="one_of") \
        .check(
        actual=result,
        expected_correct=expected_correct,
        expected_erroneous=expected_errors
    )


def test_one_of_of_other_columns_is_ignored(spark_session):
    df = spark_session.createDataFrame(
        [["a", "123"], ["bcd", "45"], ["cd", "12345"]],
        schema=two_string_columns_schema
    )
    expected_correct = spark_session.createDataFrame(
        [["cd", "12345"]],
        schema=two_string_columns_schema
    )
    expected_errors = spark_session.createDataFrame(
        [["a", "123"], ["bcd", "45"]],
        schema=two_string_columns_schema
    )
    result = ValidateSparkDataFrame(spark_session, df) \
        .one_of("col1", ["cd", "123", "45"]) \
        .execute()
    AssertValidationResult(column_name="col1", constraint_name="one_of") \
        .check(
        actual=result,
        expected_correct=expected_correct,
        expected_erroneous=expected_errors
    )


def test_should_check_all_given_columns_separately(spark_session):
    df = spark_session.createDataFrame(
        [["a", "12"], ["abcde", "56"], ["def", "123"]],
        schema=two_string_columns_schema
    )
    expected_correct = spark_session.createDataFrame([], schema=two_string_columns_schema)
    expected_errors = spark_session.createDataFrame(
        [["a", "12"], ["abcde", "56"], ["def", "123"]],
        schema=two_string_columns_schema
    )
    result = ValidateSparkDataFrame(spark_session, df) \
        .one_of("col1", ["12", "56", "def"]) \
        .one_of("col2", ["12", "56", "adcde"]) \
        .execute()
    AssertDf(result.correct_data, order_by_column="col1") \
        .contains_exactly(expected_correct.toPandas()) \
        .has_columns(["col1", "col2"])
    AssertDf(result.erroneous_data, order_by_column="col2") \
        .contains_exactly(expected_errors.toPandas()) \
        .has_columns(["col1", "col2"])
    assert result.errors == [ValidationError("col1", "one_of", 2), ValidationError("col2", "one_of", 1)]


def test_should_throw_error_if_constraint_uses_non_existing_column_one_of(spark_session):
    with pytest.raises(ValueError):
        ValidateSparkDataFrame(spark_session, empty_string_df(spark_session)) \
            .one_of("column_that_does_not_exist", []) \
            .execute()


def test_should_throw_error_if_there_are_duplicate_constraints_one_of(spark_session):
    with pytest.raises(ValueError):
        ValidateSparkDataFrame(spark_session, empty_string_df(spark_session)) \
            .one_of("col1", ["a"]) \
            .one_of("col1", ["b"]) \
            .execute()


def test_should_return_df_without_changes_if_empty_df_with_is_text_length_constraint(spark_session):
    df = empty_string_df(spark_session)
    result = ValidateSparkDataFrame(spark_session, df) \
        .has_length_between("col1", 0, 20) \
        .execute()
    AssertValidationResult(column_name="col1", constraint_name="text_length") \
        .check(
        actual=result,
        expected_correct=df,
        expected_erroneous=df
    )


def test_should_return_df_without_changes_if_all_are_shorter_than_upper_bound(spark_session):
    df = spark_session.createDataFrame([["abc"], ["def"], ["ghi"]], schema=single_string_column_schema)
    result = ValidateSparkDataFrame(spark_session, df) \
        .has_length_between("col1", 0, 20) \
        .execute()
    AssertValidationResult(column_name="col1", constraint_name="text_length") \
        .check(
        actual=result,
        expected_correct=df,
        expected_erroneous=empty_string_df(spark_session)
    )


def test_should_return_df_without_changes_if_all_are_longer_than_lower_bound(spark_session):
    df = spark_session.createDataFrame([["abcdef"], ["ghijkl"]], schema=single_string_column_schema)
    result = ValidateSparkDataFrame(spark_session, df) \
        .has_length_between("col1", 5, 20) \
        .execute()
    AssertValidationResult(column_name="col1", constraint_name="text_length") \
        .check(
        actual=result,
        expected_correct=df,
        expected_erroneous=empty_string_df(spark_session)
    )


def test_should_reject_all_rows_if_all_are_too_short_or_too_long(spark_session):
    df = spark_session.createDataFrame(
        [["abc"], ["a"], ["abcdefghi"]],
        schema=single_string_column_schema
    )
    expected_errors = spark_session.createDataFrame(
        [["abc"], ["a"], ["abcdefghi"]],
        schema=single_string_column_schema
    )
    result = ValidateSparkDataFrame(spark_session, df) \
        .has_length_between("col1", 5, 8) \
        .execute()
    AssertValidationResult(column_name="col1", constraint_name="text_length") \
        .check(
        actual=result,
        expected_correct=empty_string_df(spark_session),
        expected_erroneous=expected_errors
    )


def test_should_return_both_correct_and_incorrect_rows_length(spark_session):
    df = spark_session.createDataFrame(
        [["a"], ["abc"], ["defg"], ["hijkl"]],
        schema=single_string_column_schema
    )
    expected_correct = spark_session.createDataFrame(
        [["abc"], ["defg"]],
        schema=single_string_column_schema
    )
    expected_errors = spark_session.createDataFrame(
        [["a"], ["hijkl"]],
        schema=single_string_column_schema
    )
    result = ValidateSparkDataFrame(spark_session, df) \
        .has_length_between("col1", 3, 4) \
        .execute()
    AssertValidationResult(column_name="col1", constraint_name="text_length") \
        .check(
        actual=result,
        expected_correct=expected_correct,
        expected_erroneous=expected_errors
    )


def test_text_length_of_other_columns_is_ignored(spark_session):
    df = spark_session.createDataFrame(
        [["a", "123"], ["bcd", "45"], ["cd", "12345"]],
        schema=two_string_columns_schema
    )
    expected_correct = spark_session.createDataFrame(
        [["cd", "12345"]],
        schema=two_string_columns_schema
    )
    expected_errors = spark_session.createDataFrame(
        [["a", "123"], ["bcd", "45"]],
        schema=two_string_columns_schema
    )
    result = ValidateSparkDataFrame(spark_session, df) \
        .has_length_between("col1", 2, 2) \
        .execute()
    AssertValidationResult(column_name="col1", constraint_name="text_length") \
        .check(
        actual=result,
        expected_correct=expected_correct,
        expected_erroneous=expected_errors
    )


def test_should_check_all_given_columns_separately_length(spark_session):
    df = spark_session.createDataFrame(
        [["a", "12"], ["abcde", "56"], ["def", "123"]],
        schema=two_string_columns_schema
    )
    expected_correct = spark_session.createDataFrame([], schema=two_string_columns_schema)
    expected_errors = spark_session.createDataFrame(
        [["a", "12"], ["abcde", "56"], ["def", "123"]],
        schema=two_string_columns_schema
    )
    result = ValidateSparkDataFrame(spark_session, df) \
        .has_length_between("col1", 2, 4) \
        .has_length_between("col2", 1, 2) \
        .execute()
    AssertDf(result.correct_data, order_by_column="col1") \
        .contains_exactly(expected_correct.toPandas()) \
        .has_columns(["col1", "col2"])
    AssertDf(result.erroneous_data, order_by_column="col2") \
        .contains_exactly(expected_errors.toPandas()) \
        .has_columns(["col1", "col2"])
    assert result.errors == [ValidationError("col1", "text_length", 2), ValidationError("col2", "text_length", 1)]


def test_should_throw_error_if_constraint_uses_non_existing_column_length(spark_session):
    with pytest.raises(ValueError):
        ValidateSparkDataFrame(spark_session, empty_string_df(spark_session)) \
            .has_length_between("column_that_does_not_exist", 0, 1) \
            .execute()


def test_should_throw_error_if_there_are_duplicate_constraints_length(spark_session):
    with pytest.raises(ValueError):
        ValidateSparkDataFrame(spark_session, empty_string_df(spark_session)) \
            .has_length_between("col1", 0, 10) \
            .has_length_between("col1", 0, 5) \
            .execute()


def test_should_throw_error_if_lower_bound_is_greater_than_upper_bound_length(spark_session):
    with pytest.raises(ValueError):
        ValidateSparkDataFrame(spark_session, empty_string_df(spark_session)) \
            .has_length_between("col1", 10, 5) \
            .execute()


def test_should_throw_error_if_constraint_is_not_a_text_column(spark_session):
    with pytest.raises(ValueError):
        ValidateSparkDataFrame(spark_session, empty_integer_df(spark_session)) \
            .has_length_between("col1", 5, 10) \
            .execute()


def test_should_return_df_without_changes_if_empty_df_with_is_text_matches_regex_constraint(spark_session):
    df = empty_string_df(spark_session)
    result = ValidateSparkDataFrame(spark_session, df) \
        .text_matches_regex("col1", ".*") \
        .execute()
    AssertValidationResult(column_name="col1", constraint_name="regex_match") \
        .check(
        actual=result,
        expected_correct=df,
        expected_erroneous=df
    )


def test_should_return_df_without_changes_if_regex_matches_the_text(spark_session):
    df = spark_session.createDataFrame([["abc"], ["def"], ["ghi"]], schema=single_string_column_schema)
    result = ValidateSparkDataFrame(spark_session, df) \
        .text_matches_regex("col1", ".*") \
        .execute()
    AssertValidationResult(column_name="col1", constraint_name="regex_match") \
        .check(
        actual=result,
        expected_correct=df,
        expected_erroneous=empty_string_df(spark_session)
    )


def test_should_reject_all_rows_if_regex_match_fails(spark_session):
    df = spark_session.createDataFrame(
        [["abc"], ["a"], ["abcdefghi"]],
        schema=single_string_column_schema
    )
    expected_errors = spark_session.createDataFrame(
        [["abc"], ["a"], ["abcdefghi"]],
        schema=single_string_column_schema
    )
    result = ValidateSparkDataFrame(spark_session, df) \
        .text_matches_regex("col1", "[0-9]+") \
        .execute()
    AssertValidationResult(column_name="col1", constraint_name="regex_match") \
        .check(
        actual=result,
        expected_correct=empty_string_df(spark_session),
        expected_erroneous=expected_errors
    )


def test_should_return_both_correct_and_incorrect_rows_matches_regex(spark_session):
    df = spark_session.createDataFrame([["a"], ["abc"], ["defg"], ["hijkl"]], schema=single_string_column_schema)
    expected_correct = spark_session.createDataFrame([["abc"], ["defg"]], schema=single_string_column_schema)
    expected_errors = spark_session.createDataFrame([["a"], ["hijkl"]], schema=single_string_column_schema)
    result = ValidateSparkDataFrame(spark_session, df) \
        .text_matches_regex("col1", "^[a-z]{3,4}$") \
        .execute()
    AssertValidationResult(column_name="col1", constraint_name="regex_match") \
        .check(
        actual=result,
        expected_correct=expected_correct,
        expected_erroneous=expected_errors
    )


def test_matching_of_other_columns_is_ignored(spark_session):
    df = spark_session.createDataFrame(
        [["a", "123"], ["bcd", "45"], ["cd", "12345"]],
        schema=two_string_columns_schema
    )
    expected_correct = spark_session.createDataFrame(
        [["cd", "12345"]],
        schema=two_string_columns_schema
    )
    expected_errors = spark_session.createDataFrame(
        [["a", "123"], ["bcd", "45"]],
        schema=two_string_columns_schema
    )
    result = ValidateSparkDataFrame(spark_session, df) \
        .text_matches_regex("col1", "^[cd]+$") \
        .execute()
    AssertValidationResult(column_name="col1", constraint_name="regex_match") \
        .check(
        actual=result,
        expected_correct=expected_correct,
        expected_erroneous=expected_errors
    )


def test_should_check_all_given_columns_separately_matches_regex(spark_session):
    df = spark_session.createDataFrame(
        [["a", "12"], ["abcde", "56"], ["def", "123"]],
        schema=two_string_columns_schema
    )
    expected_correct = spark_session.createDataFrame([], schema=two_string_columns_schema)
    expected_errors = spark_session.createDataFrame(
        [["a", "12"], ["abcde", "56"], ["def", "123"]],
        schema=two_string_columns_schema
    )
    result = ValidateSparkDataFrame(spark_session, df) \
        .text_matches_regex("col1", "[0-9]+") \
        .text_matches_regex("col2", "[a-z]+") \
        .execute()
    AssertDf(result.correct_data, order_by_column="col1") \
        .contains_exactly(expected_correct.toPandas()) \
        .has_columns(["col1", "col2"])
    AssertDf(result.erroneous_data, order_by_column="col2") \
        .contains_exactly(expected_errors.toPandas()) \
        .has_columns(["col1", "col2"])
    assert result.errors == [ValidationError("col1", "regex_match", 3), ValidationError("col2", "regex_match", 3)]


def test_should_throw_error_if_constraint_uses_non_existing_column_matches_regex(spark_session):
    with pytest.raises(ValueError):
        ValidateSparkDataFrame(spark_session, empty_string_df(spark_session)) \
            .text_matches_regex("column_that_does_not_exist", '.*') \
            .execute()


def test_should_throw_error_if_there_are_duplicate_constraints_matches_regex(spark_session):
    with pytest.raises(ValueError):
        ValidateSparkDataFrame(spark_session, empty_string_df(spark_session)) \
            .text_matches_regex("column_that_does_not_exist", '.*') \
            .text_matches_regex("column_that_does_not_exist", '[a-z]*') \
            .execute()


def test_should_throw_error_if_constraint_is_not_a_text_column_matches_regex(spark_session):
    with pytest.raises(ValueError):
        ValidateSparkDataFrame(spark_session, empty_integer_df(spark_session)) \
            .text_matches_regex("col1", '[a-z]*') \
            .execute()


def test_should_return_df_without_changes_if_empty_df_with_is_unique_constraint(spark_session):
    df = empty_string_df(spark_session)
    result = ValidateSparkDataFrame(spark_session, df) \
        .is_unique("col1") \
        .execute()
    AssertValidationResult(column_name="col1", constraint_name="unique") \
        .check(
        actual=result,
        expected_correct=df,
        expected_erroneous=df
    )


def test_should_return_df_without_changes_if_all_rows_are_unique(spark_session):
    df = spark_session.createDataFrame([["abc"], ["def"], ["ghi"]], schema=single_string_column_schema)
    result = ValidateSparkDataFrame(spark_session, df) \
        .is_unique("col1") \
        .execute()
    AssertValidationResult(column_name="col1", constraint_name="unique") \
        .check(
        actual=result,
        expected_correct=df,
        expected_erroneous=empty_string_df(spark_session)
    )


@pytest.mark.xfail(strict=True)
def test_should_reject_all_rows_if_all_are_the_same(spark_session):
    df = spark_session.createDataFrame([["abc"], ["abc"], ["abc"]], schema=single_string_column_schema)
    expected_errors = spark_session.createDataFrame([["abc"]], schema=single_string_column_schema)
    result = ValidateSparkDataFrame(spark_session, df) \
        .is_unique("col1") \
        .execute()
    AssertDf(result.correct_data) \
        .is_empty() \
        .has_columns(["col1"])
    AssertDf(result.erroneous_data, order_by_column="col1") \
        .contains_exactly(expected_errors.toPandas()) \
        .has_columns(["col1"])
    assert result.errors == [ValidationError("col1", "unique", 3)]


@pytest.mark.xfail(strict=True)
def test_should_return_both_correct_and_incorrect_rows_unique(spark_session):
    df = spark_session.createDataFrame([["abc"], ["abc"], ["def"]], schema=single_string_column_schema)
    expected_correct = spark_session.createDataFrame([["def"]], schema=single_string_column_schema)
    expected_errors = spark_session.createDataFrame([["abc"]], schema=single_string_column_schema)
    result = ValidateSparkDataFrame(spark_session, df) \
        .is_unique("col1") \
        .execute()
    AssertDf(result.correct_data, order_by_column="col1") \
        .contains_exactly(expected_correct.toPandas()) \
        .has_columns(["col1"])
    AssertDf(result.erroneous_data, order_by_column="col1") \
        .contains_exactly(expected_errors.toPandas()) \
        .has_columns(["col1"])
    assert result.errors == [ValidationError("col1", "unique", 2)]


@pytest.mark.xfail(strict=True)
def test_uniqueness_of_other_columns_is_ignored(spark_session):
    df = spark_session.createDataFrame(
        [["abc", "123"], ["abc", "456"], ["def", "123"]],
        schema=two_string_columns_schema
    )
    expected_correct = spark_session.createDataFrame(
        [["def", "123"]],
        schema=two_string_columns_schema
    )
    expected_errors = spark_session.createDataFrame(
        [["abc", "123"], ["abc", "456"]],
        schema=two_string_columns_schema
    )
    result = ValidateSparkDataFrame(spark_session, df) \
        .is_unique("col1") \
        .execute()
    AssertDf(result.correct_data, order_by_column="col1") \
        .contains_exactly(expected_correct.toPandas()) \
        .has_columns(["col1", "col2"])
    AssertDf(result.erroneous_data, order_by_column="col2") \
        .contains_exactly(expected_errors.toPandas()) \
        .has_columns(["col1", "col2"])
    assert result.errors == [ValidationError("col1", "unique", 2)]


def test_should_throw_error_if_constraint_uses_non_existing_column_unique(spark_session):
    with pytest.raises(ValueError):
        ValidateSparkDataFrame(spark_session, empty_string_df(spark_session)) \
            .is_unique("column_that_does_not_exist") \
            .execute()


def test_should_throw_error_if_there_are_duplicate_constraints_unique(spark_session):
    with pytest.raises(ValueError):
        ValidateSparkDataFrame(spark_session, empty_string_df(spark_session)) \
            .is_unique("col1") \
            .is_unique("col1") \
            .execute()


def test_should_pass_empty_df_if_there_are_no_rules(spark_session):
    df = empty_string_df(spark_session)
    result = ValidateSparkDataFrame(spark_session, df).execute()
    AssertValidationResult(column_name="col1", constraint_name="") \
        .check(
        actual=result,
        expected_correct=df,
        expected_erroneous=df
    )


def test_should_pass_df_if_there_are_no_rules(spark_session):
    df = spark_session.createDataFrame([["abc"], ["def"]], schema=single_string_column_schema)
    result = ValidateSparkDataFrame(spark_session, df).execute()
    AssertValidationResult(column_name="col1", constraint_name="") \
        .check(
        actual=result,
        expected_correct=df,
        expected_erroneous=empty_string_df(spark_session)
    )
