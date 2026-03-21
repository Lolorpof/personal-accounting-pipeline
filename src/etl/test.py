from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lag, avg, round as spark_round, substring, sum as spark_sum, count, year as spark_year, dense_rank
from pyspark.sql.window import Window
from src.etl.transformations import categorize_spending

spark = SparkSession.builder.appName("Practice").getOrCreate()

df = spark.read.parquet('output/analytics/enriched_transactions/')

# 4.
"""Transactions with no merchant"""
print(f"Transactions with no merchants: {df.filter(col("merchant_id").isNull()).count()}")

# 5.
# --- Q1
df_avg_per_year = spark.read.parquet('output/analytics/avg_amount_by_year/')

window = Window.orderBy("year")

df_avg_per_year \
    .withColumn("prev_avg", lag("avg_amount").over(window)) \
    .withColumn("yoy_change",
        spark_round((col("avg_amount") - col("prev_avg")) / col("prev_avg") * 100, 4)
    ) \
    .orderBy("year") \
    .show()

# --- Q2: Category with highest total spending & fastest growth (2016 vs 2025)
df_cat = spark.read.parquet('output/analytics/monthly_by_category/')

# Highest total spending overall
df_cat.groupBy("category_name") \
    .agg(spark_sum("total_amount").alias("total_amount")) \
    .orderBy(col("total_amount").desc()) \
    .show()

# Fastest growth
df_2016 = df_cat.filter(substring("year_month", 1, 4) == "2016") \
    .groupBy("category_name").agg(spark_sum("total_amount").alias("total_2016"))
df_2025 = df_cat.filter(substring("year_month", 1, 4) == "2025") \
    .groupBy("category_name").agg(spark_sum("total_amount").alias("total_2025"))

df_2016.join(df_2025, "category_name") \
    .withColumn("growth_pct",
        spark_round((col("total_2025") - col("total_2016")) / col("total_2016") * 100, 2)) \
    .orderBy(col("growth_pct").desc()) \
    .show()

# --- Q3: Family member spending comparison
df_member = spark.read.parquet('output/analytics/yearly_by_member/')

# Total per member across all years
df_member.groupBy("member_id") \
    .agg(spark_sum("total_amount").alias("total_amount")) \
    .orderBy(col("total_amount").desc()) \
    .show()

# Top spending category per member
w_member = Window.partitionBy("member_id").orderBy(col("cat_total").desc())

df.groupBy("member_id", "category_name") \
    .agg(spark_sum("amount").alias("cat_total")) \
    .withColumn("rank", dense_rank().over(w_member)) \
    .filter(col("rank") == 1) \
    .show()

# --- Q4: Spending tier distribution
df_tiered = categorize_spending(df)
total = df_tiered.count()

# Overall distribution
df_tiered.groupBy("spending_tier") \
    .agg(count("*").alias("count")) \
    .withColumn("percentage", spark_round(col("count") / total * 100, 2)) \
    .show()

# Distribution per year
w_year = Window.partitionBy("year")
df_tiered.withColumn("year", spark_year("date")) \
    .groupBy("year", "spending_tier") \
    .agg(count("*").alias("count")) \
    .withColumn("year_total", spark_sum("count").over(w_year)) \
    .withColumn("percentage", spark_round(col("count") / col("year_total") * 100, 2)) \
    .orderBy("year", "spending_tier") \
    .show(100)

# Q8 — YoY growth rate per category
w_cat = Window.partitionBy("category_name").orderBy("year_month")

df_cat.withColumn("prev_total", lag("total_amount").over(w_cat)) \
      .withColumn("yoy_pct",
          spark_round((col("total_amount") -
  col("prev_total")) / col("prev_total") * 100, 2)) \
      .groupBy("category_name") \
      .agg(spark_round(avg("yoy_pct"),
  2).alias("avg_yoy_pct")) \
      .orderBy("avg_yoy_pct") \
      .show()

# Q10 — Needs vs Wants distribution
df.groupBy("budget_type") \
    .agg(spark_sum("amount").alias("total_spending")) \
    .withColumn("total_all", spark_sum("total_spending").over(Window.rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing))) \
    .withColumn("percentage", spark_round(col("total_spending") / col("total_all") * 100, 2)) \
    .orderBy("budget_type") \
    .show()
