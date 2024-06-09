# Databricks notebook source
# Load the gold table containing stock movement data
gold_schema = "gold_stocks"
gold_table_name = "price_movement_by_timeframe"
gold_df = spark.sql(f"SELECT * FROM {gold_schema}.{gold_table_name}")

# Define threshold values for alerting
threshold_1d_positive = 1.5  # Example threshold for positive 1-day price movement
threshold_10d_positive = 10.0  # Example threshold for positive 10-day price movement
threshold_30d_positive = 15.0  # Example threshold for positive 30-day price movement
threshold_1d_negative = -1.5  # Example threshold for negative 1-day price movement
threshold_10d_negative = -10.0  # Example threshold for negative 10-day price movement
threshold_30d_negative = -15.0  # Example threshold for negative 30-day price movement

# Filter for stocks exceeding thresholds
alerts_df = gold_df[
    (gold_df["percentage_change_1d"] > threshold_1d_positive) |
    (gold_df["percentage_change_10d"] > threshold_10d_positive) |
    (gold_df["percentage_change_30d"] > threshold_30d_positive) |
    (gold_df["percentage_change_1d"] < threshold_1d_negative) |
    (gold_df["percentage_change_10d"] < threshold_10d_negative) |
    (gold_df["percentage_change_30d"] < threshold_30d_negative)
]

print("Stocks exceeding thresholds:")
alerts_list = alerts_df.collect()
for row in alerts_list:
    stock_name = row["stock_name"]
    triggered_thresholds = []
    if row["percentage_change_1d"] > threshold_1d_positive:
        triggered_thresholds.append(f"1-day positive ({row['percentage_change_1d']})")
    if row["percentage_change_10d"] > threshold_10d_positive:
        triggered_thresholds.append(f"10-day positive ({row['percentage_change_10d']})")
    if row["percentage_change_30d"] > threshold_30d_positive:
        triggered_thresholds.append(f"30-day positive ({row['percentage_change_30d']})")
    if row["percentage_change_1d"] < threshold_1d_negative:
        triggered_thresholds.append(f"1-day negative ({row['percentage_change_1d']})")
    if row["percentage_change_10d"] < threshold_10d_negative:
        triggered_thresholds.append(f"10-day negative ({row['percentage_change_10d']})")
    if row["percentage_change_30d"] < threshold_30d_negative:
        triggered_thresholds.append(f"30-day negative ({row['percentage_change_30d']})")
    
    print(f"Stock: {stock_name}, Triggered Thresholds: {', '.join(triggered_thresholds)}")

display(alerts_df)

# COMMAND ----------


