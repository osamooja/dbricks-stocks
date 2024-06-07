# Databricks notebook source
import yfinance as yf
import pandas as pd
from pyspark.sql.functions import col
import common.etl as etl
from datetime import datetime, timedelta
from pyspark.sql import SparkSession

# Initialize Spark session
spark = SparkSession.builder.appName("StockDataPipeline").getOrCreate()

# Function to fetch data from Yahoo Finance
def fetch_yahoo_data(ticker):
    stock = yf.Ticker(ticker)
    hist = stock.history(period="max")
    return hist

# Example tickers and datasets
yahoo_tickers = ["AAPL", "MSFT", "GOOGL"]


# Fetch data
yahoo_data = {ticker: fetch_yahoo_data(ticker) for ticker in yahoo_tickers}
# quandl_data = {f"{db}_{ds}": fetch_quandl_data(db, ds) for db, ds in quandl_datasets}

# Convert Yahoo Finance data to Spark DataFrames and save as Delta tables
for ticker, data in yahoo_data.items():
    df = spark.createDataFrame(data.reset_index())
    df = etl.clean_column_names(df)
    df = etl.add_necessary_fields(df)
    df.show()
    # df.write.format("delta").mode("append").saveAsTable(f"yahoo_{ticker.lower()}")

# Convert Quandl data to Spark DataFrames and save as Delta tables
# for name, data in quandl_data.items():
#     df = spark.createDataFrame(data.reset_index())
#     df.show()
#     # df.write.format("delta").mode("overwrite").saveAsTable(f"quandl_{name.lower()}")
