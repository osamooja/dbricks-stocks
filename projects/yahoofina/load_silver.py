# Databricks notebook source
import pyspark.sql.functions as f
from pyspark.sql.window import Window
from pyspark.sql import SparkSession
import common.etl as etl


def create_silver(df, table_name):
    # Check for null values in 'close' and 'open' columns
    # df = df.dropna(subset=['close', 'open'])

    # Calculate daily returns
    df = df.withColumn("daily_return_percentage", (f.col("close") - f.col("open")) / f.col("open") * 100)
    df = df.withColumn("daily_return_percentage", f.round(f.col("daily_return_percentage"), 2))

    # Calculate moving average
    window_spec = Window.orderBy("date").rowsBetween(-6, 0)
    df = df.withColumn("7_day_moving_avg", f.mean(f.col("close")).over(window_spec))

    # Calculate volatility (std dev of returns over the past 30 days)
    window_spec_30 = Window.orderBy("date").rowsBetween(-29, 0)
    df = df.withColumn("30_day_volatility", f.stddev(f.col("daily_return_percentage")).over(window_spec_30))
    return df



# COMMAND ----------

# Step 1: List all tables in the schema
bronze_schema = "bronze_yahoofina"
silver_schema = "silver_stocks"

tables_in_schema = spark.sql(f"SHOW TABLES IN {bronze_schema}")

etl.create_schema_if_not_exists(spark, silver_schema)

# Step 2: aggregate
for table in tables_in_schema.collect():
    table_name = table['tableName']

    df = spark.sql(f"""
                   select * from {bronze_schema}.{table_name}
                   qualify row_number() over (partition by lakehouse_pk order by lakehouse_load_ts desc) = 1
                   """
    )

    df = create_silver(df, table_name)
    df.show()
    # etl.write_to_silver(df, f"{silver_schema}.{table_name}")

# COMMAND ----------


