# Databricks notebook source
from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()

# COMMAND ----------


catalog_prefix = dbutils.widgets.get("env_catalog_identifier")  # type: ignore # noqa: F821
schema_prefix = dbutils.widgets.get("schema_prefix")  # type: ignore # noqa: F821
schema = "volue"
tables = [
    "time_series_sa",
    "time_series_af",
]

# COMMAND ----------

for table in tables:
    table_name = f"{catalog_prefix}bronze.{schema_prefix}{schema}.{table}"
    spark.sql(f"DROP TABLE IF EXISTS {table_name}")

attributes_table = f"{catalog_prefix}bronze.{schema_prefix}{schema}.curves_attributes"
spark.sql(f"DELETE FROM {attributes_table} WHERE data_type IN ('SA', 'AF')")
