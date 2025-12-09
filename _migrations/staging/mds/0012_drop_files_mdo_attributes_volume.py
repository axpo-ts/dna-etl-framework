# Databricks notebook source
from pyspark.sql import SparkSession
from pyspark.sql.functions import col

spark = SparkSession.builder.getOrCreate()


# COMMAND ----------


catalog_prefix = dbutils.widgets.get("env_catalog_identifier")  # type: ignore # noqa: F821
schema_prefix = dbutils.widgets.get("schema_prefix")  # type: ignore # noqa: F821

# COMMAND ----------
volume_path = f"{catalog_prefix}staging/{schema_prefix}mds/mdo_attributes"

# List files in the volume
files_in_volume = dbutils.fs.ls(f"/Volumes/{volume_path}")  # type: ignore # noqa: F821

# Filter files based on the file_name containing 'mdo_attributes_232000253_' or 'mdo_attributes_232000254_'
files_to_delete = [
    file_in_volume
    for file_in_volume in files_in_volume
    if ("mdo_attributes_232000253_" in file_in_volume.name or "mdo_attributes_232000254_" in file_in_volume.name)
]

# Display the files to be deleted
if files_to_delete:
    display(files_to_delete)  # type: ignore # noqa: F821

# Delete the files
for file_to_delete in files_to_delete:
    file_path = file_to_delete.path
    print(f"Deleting file: {file_path}")
    dbutils.fs.rm(file_path, False)  # type: ignore # noqa: F821
