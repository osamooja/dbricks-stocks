# Databricks notebook source
import yfinance as yf
import pandas as pd
from pyspark.sql.functions import col
import common.etl as etl
from datetime import datetime, timedelta
from pyspark.sql import SparkSession

# Initialize Spark session
spark = SparkSession.builder.appName("StockDataPipeline").getOrCreate()


# COMMAND ----------

configs = etl.get_integration_configs(spark, "yahoofina")

# Function to fetch data from Yahoo Finance
def fetch_yahoo_data(ticker):
    stock = yf.Ticker(ticker)
    hist = stock.history(period="max")
    return hist

for row in configs.collect():
        source_table = row["source_table"]
        target_table = row["target_table"]
        target_schema = row["target_schema"]
        partition_key = row["partition_key"]
        primary_key_list = row["primary_key_list"]

        # Fetch data
        yahoo_data = fetch_yahoo_data(source_table)

        # Convert Yahoo Finance data to Spark DataFrames and save as Delta tables
        for ticker, data in yahoo_data.items():
            df = spark.createDataFrame(data.reset_index())
            df = etl.clean_column_names(df)
            df = etl.add_necessary_fields(df)
            df.show()
            df.write.format("delta").mode("overwrite").saveAsTable(f"{target_schema}.{target_table}")
