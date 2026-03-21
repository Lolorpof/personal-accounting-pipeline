# Define your explicit schemas here using StructType and StructField.
#
# You need three schemas:
#   - transaction_schema  (for transactions.csv)
#   - category_schema     (for categories.csv)
#   - merchant_schema     (for merchants.csv)
#
# Example:
#   from pyspark.sql.types import StructType, StructField, StringType, FloatType
#
#   my_schema = StructType([
#       StructField("id", StringType(), True),
#       StructField("value", FloatType(), True),
#   ])
#
# Hint: Look at the column descriptions in PROJECT.md.
# Hint: Think carefully about which columns should be StringType vs FloatType.
#       The CSV stores everything as text — your schema controls how Spark reads it.

from pyspark.sql.types import StructType, StructField, StringType, FloatType
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, min, max

transactions_schema = StructType([
    StructField("transaction_id", StringType(), True),
    StructField("date", StringType(), True),
    StructField("member_id", StringType(), True),
    StructField("item_name", StringType(), True),
    StructField("category_id", StringType(), True),
    StructField("merchant_id", StringType(), True),
    StructField("amount", FloatType(), True),
    StructField("payment_method", StringType(), True)
])

categories_schema = StructType([
    StructField("category_id", StringType(), True),
    StructField("category_name", StringType(), True),
    StructField("budget_type", StringType(), True)
])

merchants_schema = StructType([
    StructField("merchant_id", StringType(), True),
    StructField("merchant_name", StringType(), True),
    StructField("merchant_type", StringType(), True),
    StructField("location", StringType(), True)
])

if __name__ == "__main__":
    spark = SparkSession.builder.appName('Read').getOrCreate()

    df_transactions = spark.read.csv('data/transactions.csv', schema=transactions_schema, header=True)
    df_categories = spark.read.csv('data/categories.csv', schema=categories_schema, header=True)
    df_merchants = spark.read.csv('data/merchants.csv', schema=merchants_schema, header=True)

    # 1.1
    print(f"Total Transactions: {df_transactions.count()}")

    # 1.2
    print(f"Unique family members: {df_transactions.dropDuplicates(["member_id"]).count()}")
    print(f"Unique merchants: {df_transactions.dropDuplicates(["merchant_id"]).count()}")
    print(f"Unique categories: {df_transactions.dropDuplicates(["category_id"]).count()}")

    # 1.3
    print(f"Null or Empty amount: {df_transactions.filter(col("amount").isNull() | (col("amount") == "")).count()}")

    # 1.4
    df_transactions.select(min("date").alias("min_date"), max("date").alias("max_date")).show()
