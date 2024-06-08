# Databricks notebook source
# Load the gold table containing stock movement data
gold_schema = "gold_stocks"
gold_table_name = "price_movement_by_timeframe"
gold_df = spark.sql(f"SELECT * FROM {gold_schema}.{gold_table_name}")

# Define threshold values for alerting
threshold_1d_positive = 1.0  # Example threshold for positive 1-day price movement
threshold_10d_positive = 2.0  # Example threshold for positive 10-day price movement
threshold_30d_positive = 3.0  # Example threshold for positive 30-day price movement
threshold_1d_negative = -0.1  # Example threshold for negative 1-day price movement
threshold_10d_negative = -1.0  # Example threshold for negative 10-day price movement
threshold_30d_negative = -1.5  # Example threshold for negative 30-day price movement

# Filter for stocks exceeding thresholds
alerts_df = gold_df[
    (gold_df["avg_price_movement_1d"] > threshold_1d_positive) |
    (gold_df["avg_price_movement_10d"] > threshold_10d_positive) |
    (gold_df["avg_price_movement_30d"] > threshold_30d_positive) |
    (gold_df["avg_price_movement_1d"] < threshold_1d_negative) |
    (gold_df["avg_price_movement_10d"] < threshold_10d_negative) |
    (gold_df["avg_price_movement_30d"] < threshold_30d_negative)
]

display(alerts_df)

# COMMAND ----------


