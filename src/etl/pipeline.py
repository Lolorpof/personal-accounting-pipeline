from pyspark.sql import SparkSession
from pyspark.sql.functions import col, date_format, year, sum as spark_sum, avg, dense_rank
from pyspark.sql.window import Window

from src.etl.schemas import transactions_schema, categories_schema, merchants_schema
from src.etl.validations import filter_valid_transactions
from src.etl.transformations import enrich_with_lookups


def _write_raw_layer(spark: SparkSession) -> None:
    # Read each CSV with its schema and write to Parquet as-is
    df_transactions = spark.read.csv('data/transactions.csv', schema=transactions_schema, header=True)
    df_categories = spark.read.csv('data/categories.csv', schema=categories_schema, header=True)
    df_merchants = spark.read.csv('data/merchants.csv', schema=merchants_schema, header=True)

    df_transactions.write.mode('overwrite').parquet('output/raw/transactions/')
    df_categories.write.mode('overwrite').parquet('output/raw/categories/')
    df_merchants.write.mode('overwrite').parquet('output/raw/merchants/')


def _write_staged_layer(spark: SparkSession) -> None:
    # Read raw transactions and apply validation filters
    df_transactions = spark.read.parquet('output/raw/transactions/')
    df_valid = filter_valid_transactions(df_transactions)
    df_valid.write.mode('overwrite').parquet('output/staged/transactions/')

def _write_enriched(spark: SparkSession) -> None:
    """Read staged transactions and raw lookups, join them, and write enriched output."""
    df_transactions = spark.read.parquet('output/staged/transactions/')
    df_categories = spark.read.parquet('output/raw/categories/')
    df_merchants = spark.read.parquet('output/raw/merchants/')
    df_enriched = enrich_with_lookups(df_transactions, df_categories, df_merchants)
    df_enriched.write.mode("overwrite").parquet('output/analytics/enriched_transactions/')

def _write_monthly_by_category(spark: SparkSession) -> None:
    """Aggregate total spend per category per month and write to analytics layer."""
    df = spark.read.parquet('output/analytics/enriched_transactions/')
    df.groupBy(date_format("date", "yyyy-MM").alias("year_month"), "category_name") \
      .agg(spark_sum("amount").alias("total_amount")) \
      .write.mode("overwrite").parquet('output/analytics/monthly_by_category/')


def _write_yearly_by_member(spark: SparkSession) -> None:
    """Aggregate total spend per member per year and write to analytics layer."""
    df = spark.read.parquet('output/analytics/enriched_transactions/')
    df.groupBy(year("date").alias("year"), "member_id") \
      .agg(spark_sum("amount").alias("total_amount")) \
      .write.mode("overwrite").parquet('output/analytics/yearly_by_member/')


def _write_top_merchants_by_year(spark: SparkSession) -> None:
    """Rank merchants by total spend per year and write top 10 per year to analytics layer."""
    df = spark.read.parquet('output/analytics/enriched_transactions/')
    window = Window.partitionBy("year").orderBy(col("total_amount").desc())
    df.groupBy(year("date").alias("year"), "merchant_name") \
      .agg(spark_sum("amount").alias("total_amount")) \
      .withColumn("rank", dense_rank().over(window)) \
      .filter(col("rank") <= 10) \
      .write.mode("overwrite").parquet('output/analytics/top_merchants_by_year/')


def _write_avg_amount_by_year(spark: SparkSession) -> None:
    """Aggregate average transaction amount per year and write to analytics layer."""
    df = spark.read.parquet('output/analytics/enriched_transactions/')
    df.groupBy(year("date").alias("year")) \
      .agg(avg("amount").alias("avg_amount")) \
      .write.mode("overwrite").parquet('output/analytics/avg_amount_by_year/')


def run_pipeline() -> None:
    spark = (SparkSession.builder
             .appName('PersonalAccountingPipeline')
             .config('spark.driver.memory', '2g')
             .config('spark.sql.shuffle.partitions', '4')
             .getOrCreate())

    _write_raw_layer(spark)
    _write_staged_layer(spark)
    _write_enriched(spark)
    _write_monthly_by_category(spark)
    _write_yearly_by_member(spark)
    _write_top_merchants_by_year(spark)
    _write_avg_amount_by_year(spark)


if __name__ == "__main__":
    run_pipeline()
