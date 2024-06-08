# Databricks notebook source
from pyspark.sql import functions as f
from pyspark.sql.window import Window
import common.etl as etl
from datetime import datetime, timedelta
import pandas as pd


# Define the timeframes in terms of days
timeframes = [1, 10, 30]

def create_price_movements(df):
    for timeframe in timeframes:
        # Define the window spec for the rolling calculation
        window_spec = Window.partitionBy("stock_name").orderBy(f.desc("date")).rowsBetween(-timeframe, 0)

        # Calculate price movement for each timeframe
        first_close = f.first(df["close"]).over(window_spec)
        last_close = f.last(df["close"]).over(window_spec)
        
        price_movement = ((last_close - first_close) / first_close) * 100
        df = df.withColumn(f"price_movement_{timeframe}d", price_movement)
    
    return df


# COMMAND ----------

# Combine all silver tables into one DataFrame
silver_schema = "silver_stocks"
gold_schema = "gold_stocks"

tables_in_silver_schema = spark.sql(f"show tables in {silver_schema}")

etl.create_schema_if_not_exists(spark, gold_schema)

# Combine all silver tables into one DataFrame
combined_df = None
for table in tables_in_silver_schema.collect():
    table_name = table['tableName']
    df = spark.sql(f"""
                   select source_table as stock_name, *
                   from {silver_schema}.{table_name}
                   """
    )
    df = df.drop("source_table")
    if combined_df is None:
        combined_df = df
    else:
        combined_df = combined_df.union(df)
df = combined_df

# Calculate price change
windowSpec = Window.partitionBy("stock_name").orderBy(f.col("date").desc())
# Calculate price change and percentage change
df = df.withColumn("price_change", f.col("close") - f.col("open"))
df = df.withColumn("percentage_change", f.round((f.col("price_change") / f.col("open") * 100), 2))

# Filter to keep only the last two records for each stock
df = df.withColumn("rank", f.row_number().over(windowSpec)).filter(f.col("rank") <= 3).drop("rank")
df = df.select("stock_name", "date", "open", "close", "percentage_change")
display(df)

# Write the aggregated DataFrame to the gold schema
gold_table_name = "price_movement_by_timeframe"
# display(df)
# etl.write_to_table(df, f"{gold_schema}.{gold_table_name}")

print("Gold table for price movement created successfully.")

# COMMAND ----------


