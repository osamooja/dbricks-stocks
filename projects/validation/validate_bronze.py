# Databricks notebook source
from pyspark.sql import functions as f

def validate_bronze_tables(bronze_schema):
    # List all tables in the bronze schema
    tables_in_bronze = spark.sql(f"SHOW TABLES IN {bronze_schema}")

    validation_results = []

    # Iterate through each table
    for table_row in tables_in_bronze.collect():
        table_name = table_row['tableName']
        table_validation_result = validate_bronze_table(f"{bronze_schema}.{table_name}")
        validation_results.append((table_name, table_validation_result))

    return validation_results

def validate_bronze_tables(bronze_schema):
    # List all tables in the bronze schema
    tables_in_bronze = spark.sql(f"SHOW TABLES IN {bronze_schema}")

    validation_results = []

    # Iterate through each table
    for table_row in tables_in_bronze.collect():
        table_name = table_row['tableName']
        table_validation_result = validate_bronze_table(f"{bronze_schema}.{table_name}")
        validation_results.append((table_name, table_validation_result))

    return validation_results

def validate_bronze_table(bronze_table_name):
    # Read the bronze table
    bronze_df = spark.table(bronze_table_name)

    # Check for null or zero values in specified columns
    columns_to_check = ['open', 'high', 'low', 'close', 'volume']

    null_counts = {}

    for column in columns_to_check:
        null_count = bronze_df.where(f.col(column).isNull()).count()
        zero_count = bronze_df.where(f.col(column) == 0).count()

        # If null or zero values are found, accumulate null count
        if null_count > 0 or zero_count > 0:
            null_counts[column] = {'null_count': null_count, 'zero_count': zero_count}

    # If any failures occurred, return False along with null counts
    if null_counts:
        print(f"Validation failed for table '{bronze_table_name}'. Null counts for failed columns:")
        for column, counts in null_counts.items():
            print(f"Column '{column}': Null Count = {counts['null_count']}, Zero Count = {counts['zero_count']}")
        return False
    return True

# Example usage: Replace 'bronze_yahoofina' with your actual bronze schema name
bronze_schema = ["bronze_yahoofina", "bronze_quandlfina"]

try:
    for schema in bronze_schema:
        # Validate all tables in the schema
        print(f"Validating schema: {schema}")

        all_validation_results = validate_bronze_tables(schema)
        

        # Check if any table failed validation
        failed_tables = [(table_name, validation_result) for table_name, validation_result in all_validation_results if not validation_result]
        if failed_tables:
            print("Validation failed for the following tables:")
            for table_name, validation_result in failed_tables:
                print(f"Table: {schema}.{table_name}, Validation Result: {validation_result}")  # Print failed tables and their validation results
        else:
            print("Validation passed for all tables. No null or zero values found.")

except ValueError as e:
    print("Validation failed:", e)
    # Exit with a non-zero status code to indicate failure
    exit(1)


# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC select * from bronze_yahoofina.aapl where volume = 0

# COMMAND ----------


