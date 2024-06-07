# Databricks notebook source
import re
import pyspark.sql.functions as f
from pyspark.sql import DataFrame


def clean_column_names(df) -> DataFrame :
    """
    Removes or replaces invalid characters from column names of a Spark DataFrame and converts them to lowercase.
    Invalid characters are spaces, commas, semicolons, curly braces, parentheses, tabs, and equals signs.

    Args:
        df (DataFrame): The DataFrame to clean

    Returns:
        DataFrame: The DataFrame with cleaned and lowercase column names
    """
    # Invalid characters to be removed or replaced
    replace_char = re.compile(r"[ /,;{}\(\)\n\t=]+")
    remove_char = re.compile(r"[-:?]+")

    # Clean and lowercase column names
    cleaned_columns = [re.sub(remove_char, "", column_name) for column_name in df.columns]
    cleaned_columns = [re.sub(replace_char, "_", name).lower() for name in cleaned_columns]

    # Rename columns
    for old_name, new_name in zip(df.columns, cleaned_columns):
        df = df.withColumnRenamed(old_name, new_name)

    return df

def add_necessary_fields(df) -> DataFrame:
    df = df.withColumn("load_ts", f.current_timestamp())
    
    return df