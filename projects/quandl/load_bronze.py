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