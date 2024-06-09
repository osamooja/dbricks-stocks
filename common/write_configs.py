import os
from pyspark.sql import SparkSession, functions as f
from pyspark.sql.types import StructType, StructField, StringType, ArrayType
import yaml
from pyspark.sql import SparkSession
import common.etl as etl


def write_project_configs_to_db(spark: SparkSession) -> None:
    """
    Loops through the 'projects' directory and writes the config.yml files to the integration_config schema. Target table is deducted from the config.yml file.
    Only config files having an attribute 'target_config_table' are written to the database.

    Args:
        spark (SparkSession): SparkSession object
    Returns:
        None
    """

    # Attribute names
    config_file_name = "config.yml"  # Name of the config file
    target_config_table = "target_config_table"  # Name of the target table attribute in the config file

    current_directory =  os.getcwd()
    parent_directory = os.path.dirname(current_directory)
    base_directory = f"{parent_directory}/projects"

    # init result list
    result_list = []

    # Loop through subfolders under the 'projects' directory
    for folder in os.listdir(base_directory):

        folder_path = os.path.join(base_directory, folder)

        # Check if it's a directory and if it contains a 'config.yml' file
        if os.path.isdir(folder_path) and config_file_name in os.listdir(folder_path):
            config_file_path = os.path.join(folder_path, config_file_name)

            # Read the 'config.yml' file
            with open(config_file_path, 'r') as file:
                config_data = yaml.safe_load(file)

            try:
                source_info_data = config_data["source_information"]
                source_info_df = spark.createDataFrame([source_info_data])

                # get some basic information which is needed
                target_table = config_data[target_config_table]
                object_info_data = config_data["object_information"]

                #object_info_schemas = []
                object_info_dfs = []

                # Iterate through the list of dictionaries in object_information
                for object_info_dict in object_info_data:
                    # create a schema based on the keys in the dictionary
                    object_info_schema = StructType([StructField(field, StringType(), True) for field in object_info_dict.keys()])
                    
                    # create a DF using the schema and the dict data
                    object_info_df = spark.createDataFrame([object_info_dict], schema=object_info_schema)
                    
                    # append DFs
                    object_info_dfs.append(object_info_df)

                # combine all object_information DFs into one
                combined_object_info_df = object_info_dfs[0]
                for df in object_info_dfs[1:]:
                    combined_object_info_df = combined_object_info_df.union(df)

                # add common key to both dfs for merging
                common_key = 'key'
                source_info_df = source_info_df.withColumn(common_key, f.lit(1))
                target_tables_df = combined_object_info_df.withColumn(common_key, f.lit(1))

                # merge the dfs
                df = source_info_df.join(target_tables_df, on=common_key, how='inner')

                # drop the common key
                df = df.drop(common_key)

                etl.create_schema_if_not_exists(spark, "integration_configs")

                print(f"Writing to: integration_configs.{target_table}")
                df.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(f"integration_configs.{target_table}")
            except Exception as e:
                print(e)


def main():
    spark = SparkSession.builder.appName("configs").getOrCreate()
    write_project_configs_to_db(spark)


if __name__ == "__main__":
    main()