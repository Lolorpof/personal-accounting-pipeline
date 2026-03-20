import pytest
from pyspark.sql import SparkSession
from chispa.dataframe_comparer import assert_df_equality
from src.etl.validations import filter_valid_transactions


@pytest.fixture(scope="session")
def spark():
    return SparkSession.builder.master("local").appName("Testing").getOrCreate()


# TODO: Add a test for filter_valid_transactions — removes null amounts
# Hint: Include rows with None amount, verify they are removed
def test_filter_valid_transactions_removes_null_amounts(spark):
    input_df = spark.createDataFrame(
        [
            ("t1", "2020-01-01", "m1", "item1", "c1", "mer1", 50.0, "credit"),
            ("t2", "2020-01-02", "m2", "item2", "c2", "mer2", None, "cash"),
        ],
        ["transaction_id", "date", "member_id", "item_name", "category_id", "merchant_id", "amount", "payment_method"]
    )
    expected_df = spark.createDataFrame(
        [
            ("t1", "2020-01-01", "m1", "item1", "c1", "mer1", 50.0, "credit"),
        ],
        ["transaction_id", "date", "member_id", "item_name", "category_id", "merchant_id", "amount", "payment_method"]
    )
    result_df = filter_valid_transactions(input_df)
    assert_df_equality(result_df, expected_df, ignore_row_order=True)


# TODO: Add a test for filter_valid_transactions — removes out-of-range dates
# Hint: Include a row with date "2010-05-15", verify it is removed
def test_filter_valid_transactions_removes_out_of_range_dates(spark):
    input_df = spark.createDataFrame(
        [
            ("t1", "2020-01-01", "m1", "item1", "c1", "mer1", 50.0, "credit"),
            ("t2", "2010-05-15", "m2", "item2", "c2", "mer2", 40.0, "cash"),
        ],
        ["transaction_id", "date", "member_id", "item_name", "category_id", "merchant_id", "amount", "payment_method"]
    )
    expected_df = spark.createDataFrame(
        [
            ("t1", "2020-01-01", "m1", "item1", "c1", "mer1", 50.0, "credit"),
        ],
        ["transaction_id", "date", "member_id", "item_name", "category_id", "merchant_id", "amount", "payment_method"]
    )
    result_df = filter_valid_transactions(input_df)
    assert_df_equality(result_df, expected_df, ignore_row_order=True)


# TODO: Add a test for filter_valid_transactions — keeps refunds (negative amounts)
# Hint: Include a row with amount -25.0, verify it is kept
def test_filter_valid_transactions_keeps_refunds(spark):
    input_df = spark.createDataFrame(
        [
            ("t1", "2020-01-01", "m1", "item1", "c1", "mer1", 50.0, "credit"),
            ("t2", "2021-03-10", "m2", "item2", "c2", "mer2", -25.0, "cash"),
        ],
        ["transaction_id", "date", "member_id", "item_name", "category_id", "merchant_id", "amount", "payment_method"]
    )
    expected_df = spark.createDataFrame(
        [
            ("t1", "2020-01-01", "m1", "item1", "c1", "mer1", 50.0, "credit"),
            ("t2", "2021-03-10", "m2", "item2", "c2", "mer2", -25.0, "cash"),
        ],
        ["transaction_id", "date", "member_id", "item_name", "category_id", "merchant_id", "amount", "payment_method"]
    )
    result_df = filter_valid_transactions(input_df)
    assert_df_equality(result_df, expected_df, ignore_row_order=True)
