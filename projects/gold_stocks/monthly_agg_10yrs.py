# Databricks notebook source
from pyspark.sql import SparkSession, Window
import pyspark.sql.functions as f
import common.etl as etl
import datetime

# Initialize Spark session
spark = SparkSession.builder.appName("GoldLayerCreation").getOrCreate()


# COMMAND ----------

def create_gold_layer(df):
    # Get current year and calculate the year for filtering
    current_year = datetime.datetime.now().year
    start_year = current_year - 10

    # Filter for the last 5 years
    df = df.filter(f.year("date") >= start_year)

    # Aggregate data to monthly level for each stock
    df = df.groupBy("stock_name", f.year("date").alias("year"), f.month("date").alias("month")).agg(
        f.round(f.mean("open"), 4).alias("avg_open"),
        f.round(f.mean("high"), 4).alias("avg_high"),
        f.round(f.mean("low"), 4).alias("avg_low"),
        f.round(f.mean("close"), 4).alias("avg_close"),
        f.round(f.mean("volume"), 4).alias("avg_volume"),
        f.round(f.max("high"), 4).alias("max_high"),
        f.round(f.min("low"), 4).alias("min_low"),
        f.round(f.mean("percentage_change_1d"), 2).alias("avg_percentage_change_1d"),
        f.round(f.mean("7_day_moving_avg"), 4).alias("avg_7_day_moving_avg"),
        f.round(f.mean("30_day_volatility"), 4).alias("avg_30_day_volatility")
    ).orderBy("stock_name", "year", "month", ascending=[True, False, False])

    return df

# COMMAND ----------

# Combine all silver tables into one DataFrame
silver_schema = "silver_stocks"
gold_schema = "gold_stocks"

tables_in_silver_schema = spark.sql(f"show tables in {silver_schema}")

etl.create_schema_if_not_exists(spark, gold_schema)

combined_df = None
for table in tables_in_silver_schema.collect():
    table_name = table['tableName']
    df = spark.sql(f"""
                   select   source_table as stock_name
                            , date
                            , open
                            , high
                            , low
                            , close
                            , volume
                            , percentage_change_1d
                            , 7_day_moving_avg
                            , 30_day_volatility
                   from {silver_schema}.{table_name}
                   """
    )
    df = df.drop("source_table")
    if combined_df is None:
        combined_df = df
    else:
        combined_df = combined_df.union(df)

# Create gold layer DataFrame
df = create_gold_layer(combined_df)
df = df.withColumn("last_updated_UTC", f.current_timestamp())

# Write the gold layer DataFrame to the gold schema
gold_table_name = "monthly_agg_10yrs"
etl.write_to_table(df, f"{gold_schema}.{gold_table_name}")

print("Gold table for monthly history created successfully.")
