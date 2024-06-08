# Databricks notebook source
# Step 1: List all tables in the schema
schema = "silver_stocks"

tables_in_schema = spark.sql(f"SHOW TABLES IN {schema}")
display(tables_in_schema)

# Step 2: Drop all tables in the schema
for table in tables_in_schema.collect():
    table_name = table['tableName']
    drop_table_sql = f"DROP TABLE IF EXISTS {schema}.{table_name}"
    print(drop_table_sql)  # Print the DROP statement for reference
    spark.sql(drop_table_sql)
# spark.sql(f"drop schema if exists {schema}")

# COMMAND ----------


