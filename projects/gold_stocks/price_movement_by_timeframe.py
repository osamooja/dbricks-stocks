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
        window_spec = Window.partitionBy("stock_name").orderBy("date").rowsBetween(-timeframe, 0)
        
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

# Convert date column to date format
df = combined_df.withColumn("date", f.to_date("date"))
today = datetime.today()
one_day_ago = today - timedelta(days=1)
ten_days_ago = today - timedelta(days=10)
thirty_days_ago = today - timedelta(days=30)

# Filter DataFrame
one_day_data = df.filter(f.col("date") == one_day_ago)
display(one_day_data)
ten_days_data = df.filter(f.col("date") >= ten_days_ago)
display(ten_days_data)
thirty_days_data = df.filter(f.col("date") >= thirty_days_ago)
display(thirty_days_data)
# Create DataFrame with price movements
# df = create_price_movements(combined_df)

# Aggregate data to stock level
# df = df.groupBy("stock_name").agg(
#     f.round(f.avg("price_movement_1d"), 2).alias("avg_price_movement_1d"),
#     f.round(f.avg("price_movement_10d"), 2).alias("avg_price_movement_10d"),
#     f.round(f.avg("price_movement_30d"), 2).alias("avg_price_movement_30d"),
#     f.round(f.last("price_movement_1d"), 2).alias("last_price_movement_1d"),
#     f.round(f.last("price_movement_10d"), 2).alias("last_price_movement_10d"),
#     f.round(f.last("price_movement_30d"), 2).alias("last_price_movement_30d")
# )

# Write the aggregated DataFrame to the gold schema
gold_table_name = "price_movement_by_timeframe"
# display(df)
# etl.write_to_table(df, f"{gold_schema}.{gold_table_name}")

print("Gold table for price movement created successfully.")

# COMMAND ----------


