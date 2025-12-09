# Databricks notebook source
from databricks.sdk.runtime import dbutils
from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()

# COMMAND ----------
catalog_prefix = dbutils.widgets.get("env_catalog_identifier")  # type: ignore
schema_prefix = dbutils.widgets.get("schema_prefix")  # type: ignore

# COMMAND ----------

# Read the physical_flow table
table_name = f"{catalog_prefix}bronze.{schema_prefix}nordpool.physical_flow"
df = spark.read.table(table_name)

# Merge keys for deduplication
merge_keys = ["deliveryStart", "deliveryEnd", "area", "deliveryArea", "connection"]

# Drop duplicates based on the merge keys
df_deduplicated = df.dropDuplicates(merge_keys)

# Overwrite the existing table with the deduplicated DataFrame
df_deduplicated.write.mode("overwrite").saveAsTable(table_name)
