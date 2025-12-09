# Databricks notebook source
from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()


# COMMAND ----------


catalog_prefix = dbutils.widgets.get("env_catalog_identifier")  # type: ignore # noqa: F821
schema_prefix = dbutils.widgets.get("schema_prefix")  # type: ignore # noqa: F821

# COMMAND ----------
volume_path = f"{catalog_prefix}staging/{schema_prefix}swissgrid/activation_signal"

# List files in the volume
files_in_volume = ["Aug_23 (2).csv", "Dez_23 (2).csv", "Jan_24 (1).csv", "Nov_23 (3).csv", "Okt_23 (2).csv"]

# List files in the volume
files_to_delete = []
for file_name in files_in_volume:
    file_path = f"/Volumes/{volume_path}/{file_name}"
    try:
        if dbutils.fs.ls(file_path):  # type: ignore # noqa: F821
            files_to_delete.append(file_path)
    except Exception:
        print(f"File not found: {file_path}")

# Delete the files
for file_to_delete in files_to_delete:
    print(f"Deleting file: {file_to_delete}")
    dbutils.fs.rm(file_to_delete, False)  # type: ignore # noqa: F821
