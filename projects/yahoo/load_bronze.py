# Databricks notebook source
import yfinance as yf
from pyspark.sql.functions import col
import common.etl as etl
from pyspark.sql import SparkSession

# Initialize Spark session
spark = SparkSession.builder.appName("StockDataPipeline").getOrCreate()


# COMMAND ----------

def fetch_yahoo_data(ticker):
    """
    Fetch historical stock data from Yahoo Finance.

    Parameters:
    ticker (str): The stock ticker symbol.

    Returns:
    pd.DataFrame: Historical stock data.
    """
    stock = yf.Ticker(ticker)
    data = stock.history(period="max")
    return data


# COMMAND ----------

# Load integration configurations
source_system = "yahoofina"
configs = etl.get_integration_configs(spark, source_system)
target_schema = configs.select("target_schema").first().target_schema

# Create schema if it does not exist
etl.create_schema_if_not_exists(spark, target_schema)

exceptions = []
for row in configs.collect():
    source_table = row["source_table"]
    target_table = row["target_table"]

    try:
        # Fetch data
        yahoo_data = fetch_yahoo_data(source_table)

        if yahoo_data.empty:
            print(f"No data found for: {source_table}. Skipping.")
            continue

        # Convert Yahoo Finance data to Spark DataFrame
        df = spark.createDataFrame(yahoo_data.reset_index())
        df = etl.clean_column_names(df)
        df = etl.add_necessary_fields(df)
        df.show()
        # Write DataFrame to Delta table
        # etl.write_to_bronze(df, f"{target_schema}.{target_table}")

    except Exception as e:
        error_message = str(e)
        print(f"Loading {source_table} to {target_table} failed with error: {error_message}")
        print("Continuing loading remaining tables.")
        exceptions.append(e)

if exceptions:
    raise Exception(f"One or more loads failed: {exceptions}")

# COMMAND ----------


