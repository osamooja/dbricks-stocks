# Databricks notebook source
from pyspark.sql import functions as f
from pyspark.sql.window import Window
import common.etl as etl

# COMMAND ----------

timeframes = [10, 30]

def price_movement_in_timeframe(df):
    for timeframe in timeframes:
        # Calculate price change over a 30-day period
        windowSpec = Window.partitionBy("stock_name").orderBy(f.col("date").desc())
        df = df.withColumn(f"prev_close_{timeframe}d", f.lag("close", -timeframe).over(windowSpec))

        # Filter out rows with null values in prev_close_30
        df = df.filter(f.col(f"prev_close_{timeframe}d").isNotNull())

        # Calculate price_change_30 and percentage_change_30
        df = df.withColumn(f"price_change_{timeframe}d", f.col("close") - f.col(f"prev_close_{timeframe}d"))
        df = df.withColumn(f"percentage_change_{timeframe}d", (f.col(f"price_change_{timeframe}d") / f.col(f"prev_close_{timeframe}d")) * 100)
        df = df.withColumn(f"percentage_change_{timeframe}d", f.round(f.col(f"percentage_change_{timeframe}d"), 2))

        # Filter to keep only the last record for each stock
    df = df.withColumn("rank", f.row_number().over(windowSpec)).filter(f.col("rank") == 1).drop("rank")
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

df = price_movement_in_timeframe(df)

df = df.select("stock_name", "date", "open", "close", "percentage_change_1d", "prev_close_10d", "price_change_10d", "percentage_change_10d", "prev_close_30d", "price_change_30d", "percentage_change_30d")

# Write the aggregated DataFrame to the gold schema
gold_table_name = "price_movement_by_timeframe"
etl.write_to_table(df, f"{gold_schema}.{gold_table_name}")

print("Gold table for price movement created successfully.")
