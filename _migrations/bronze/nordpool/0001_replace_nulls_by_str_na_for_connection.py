# Databricks notebook source
from databricks.sdk.runtime import dbutils
from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()

# COMMAND ----------
catalog_prefix = dbutils.widgets.get("env_catalog_identifier")  # type: ignore
schema_prefix = dbutils.widgets.get("schema_prefix")  # type: ignore

# COMMAND ----------

# Read tables and replace null values in the 'connection' columns with 'N/A'
table_name = f"{catalog_prefix}bronze.{schema_prefix}nordpool.physical_flow"

query = f"UPDATE {table_name} SET connection = 'N/A' WHERE connection IS NULL;"
print(query)
spark.sql(query)

query = f"ALTER TABLE {table_name} ALTER COLUMN connection SET NOT NULL;"
print(query)
spark.sql(query)

print("done")
