# Databricks notebook source
import pyspark.sql.functions as f
from pyspark.sql.window import Window
from pyspark.sql import SparkSession
import common.etl as etl


def create_silver(df, table_name):
    # Check for null values in 'close' and 'open' columns
    df = df.filter((f.col("close").isNotNull()) & (f.col("open").isNotNull()) & (f.col("close") != 0) & (f.col("open") != 0))

    # Calculate daily returns
    df = df.withColumn("percentage_change_1d", (f.col("close") - f.col("open")) / f.col("open") * 100)
    df = df.withColumn("percentage_change_1d", f.round(f.col("percentage_change_1d"), 2))

    # Calculate moving average
    window_spec = Window.orderBy("date").rowsBetween(-6, 0)
    df = df.withColumn("7_day_moving_avg", f.mean(f.col("close")).over(window_spec))

    # Calculate volatility (std dev of returns over the past 30 days)
    window_spec_30 = Window.orderBy("date").rowsBetween(-29, 0)
    df = df.withColumn("30_day_volatility", f.stddev(f.col("percentage_change_1d")).over(window_spec_30))
    return df



# COMMAND ----------

bronze_schemas = ["bronze_yahoofina", "bronze_quandlfina"]
silver_schema = "silver_stocks"

# Function to list tables in a schema
def list_tables(schema_name):
    return spark.sql(f"show tables in {schema_name}")

# List tables in bronze schemas
tables_in_bronze = []
for schema in bronze_schemas:
    tables_in_bronze.extend(list_tables(schema).collect())

# Ensure the silver schema exists
etl.create_schema_if_not_exists(spark, silver_schema)

# Step 2: Aggregate
for table in tables_in_bronze:
    table_name = table['tableName']
    schema_name = table['database']

    df = spark.sql(f"""
                   select *
                   from {schema_name}.{table_name}
                   qualify row_number() over (partition by lakehouse_pk order by lakehouse_load_ts desc) = 1
                   """
    )

    df = create_silver(df, table_name)

    if schema_name == "bronze_yahoofina":
        target_path = f"{silver_schema}.yahoofina_{table_name}"
    elif schema_name == "bronze_quandlfina":
        target_path = f"{silver_schema}.quandlfina_{table_name}"
    else:
        print(f"Unkown schema_name: {schema_name}")

    etl.write_to_table(df, target_path)

print("Silver created succesfully")
