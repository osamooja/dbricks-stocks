import re
import pyspark.sql.functions as f
from pyspark.sql import DataFrame
from pyspark.sql import SparkSession
from datetime import datetime, timezone


def clean_column_names(df) -> DataFrame:
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


def add_necessary_fields(df, pk_list, table_name) -> DataFrame:
    df = df.withColumn("source_table", f.lit(table_name))
    df = df.withColumn("lakehouse_load_ts", f.current_timestamp())
    df = df.withColumn("lakehouse_pk", f.concat_ws('_', *pk_list))
    
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
        select  *,
                split(primary_key, ', ') as pk_list
        from integration_configs.{source_system}
        """
    )
    df = df.drop("primary_key")
    return df


def write_to_table(df: DataFrame, path: str, mode: str = "overwrite") -> None:
    """
    Write to bronze
    Args:
        DataFrame containing raw data
        Path in bronze to save table into
        Mode to use in df write
    Return:
        None
    """
    df.write.format("delta").mode(mode).option("overwriteSchema", "true").saveAsTable(path)

    return None




