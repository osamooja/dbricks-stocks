# Databricks notebook source
import quandl as qu
import pandas as pd
from pyspark.sql.functions import col


# Set your Quandl API key
qu.ApiConfig.api_key = "MvDgQDhTTE8sMYYegoz_"

quandl_datasets = [("WIKI", "AAPL"), ("WIKI", "MSFT"), ("WIKI", "GOOGL")]


# Function to fetch data from Quandl
def fetch_quandl_data(database_code, dataset_code):
    data = qu.get(f"{database_code}/{dataset_code}")
    return data

# quandl_data = {f"{db}_{ds}": fetch_quandl_data(db, ds) for db, ds in quandl_datasets}

# Convert Quandl data to Spark DataFrames and save as Delta tables
# for name, data in quandl_data.items():
#     df = spark.createDataFrame(data.reset_index())
#     df.show()
#     # df.write.format("delta").mode("overwrite").saveAsTable(f"quandl_{name.lower()}")
