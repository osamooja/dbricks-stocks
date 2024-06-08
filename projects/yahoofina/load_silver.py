# Databricks notebook source
import pyspark.sql.functions as f
from pyspark.sql.window import Window
import common.etl as etl
from pyspark.sql import SparkSession

# Initialize Spark session
spark = SparkSession.builder.appName("StockDataPipeline").getOrCreate()
silver_schema = "silver_yahoofina"
table_name = "aapl"

def create_silver(table_name):
    df = spark.sql(f"select * from bronze_yahoofina.{table_name}")

    # Check for null values in 'close' and 'open' columns
    df = df.dropna(subset=['close', 'open'])

    # Calculate moving average
    window_spec = Window.orderBy("date").rowsBetween(-6, 0)
    df = df.withColumn("7_day_moving_avg", f.mean(f.col("close")).over(window_spec))

    # Calculate daily returns
    df = df.withColumn("daily_return_percentage", (f.col("close") - f.col("open")) / f.col("open") * 100)
    df = df.withColumn("daily_return_percentage", f.round(f.col("daily_return_percentage"), 2))

    # Calculate volatility (std dev of returns over the past 30 days)
    window_spec_30 = Window.orderBy("date").rowsBetween(-29, 0)
    df = df.withColumn("30_day_volatility", f.stddev(f.col("daily_return_percentage")).over(window_spec_30))
    return df

df = create_silver(table_name)
df.show()
etl.create_schema_if_not_exists(spark, silver_schema)
etl.write_to_silver(df, f"{silver_schema}.{table_name}")

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from silver_stocks.aapl

# COMMAND ----------


