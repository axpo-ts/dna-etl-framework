# Databricks notebook source
from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()

# COMMAND ----------
# https://github.com/axpo-ts/dna-platform-project-repo/issues/254

catalog_prefix = dbutils.widgets.get("env_catalog_identifier")  # type: ignore # noqa: F821
schema_prefix = dbutils.widgets.get("schema_prefix")  # type: ignore # noqa: F821

# COMMAND ----------

# COMMAND ----------
volume_path = f"{catalog_prefix}staging/{schema_prefix}swissgrid/activation_signal"

# List files in the volume
files_in_volume = ["Aug_23 (2).csv", "Dez_23 (2).csv", "Jan_24 (1).csv", "Nov_23 (3).csv", "Okt_23 (2).csv"]

files_to_delete = [f"/Volumes/{volume_path}/{file_name}" for file_name in files_in_volume]  # type: ignore

files_to_delete = [file_to_delete.replace(" ", "%20") for file_to_delete in files_to_delete]

files_to_delete_string = ", ".join([f"'{file_to_delete}'" for file_to_delete in files_to_delete])

table_name = f"{catalog_prefix}bronze.{schema_prefix}swissgrid.activation_signal"
# Check if the table has any of the specified files and delete them if they exist
df_bronze = spark.sql(f"SELECT count(*) AS total FROM {table_name} WHERE file_path IN ({files_to_delete_string})")

total_count = df_bronze.collect()[0]["total"]
print("Total count: " + str(total_count))
if total_count > 0:
    sql_stmt = f"DELETE FROM {table_name} WHERE file_path IN ({files_to_delete_string})"
    print(sql_stmt)
    spark.sql(sql_stmt)
