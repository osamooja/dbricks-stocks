import re
import pyspark.sql.functions as f
from pyspark.sql import DataFrame
from pyspark.sql import SparkSession


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


def create_schema_if_not_exists(spark: SparkSession, schema_name: str) -> None:
    """
    Create a schema if it does not exist.

    Args:
        spark: SparkSession to use.
        schema_name: Schema to create (if it does not exist). Use fully qualified name (e.g. "my_schema").

    Returns:
        None
    """
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {schema_name}")


def get_integration_configs(spark: SparkSession, source_system: str) -> DataFrame:
    """
    Reads the configs from `integration_configs.{source_system}`

    Args:
        source_system: Source system name (e.g. "yahoofina")

    Returns:
        DataFrame containing the configs for the source system
    """
    df = spark.sql(
        f"""
        select *
        from integration_configs.{source_system}
        """
    )
    return df



