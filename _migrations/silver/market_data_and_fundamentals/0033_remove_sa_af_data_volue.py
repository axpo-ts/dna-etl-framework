# Databricks notebook source
from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()

# COMMAND ----------


catalog_prefix = dbutils.widgets.get("env_catalog_identifier")  # type: ignore # noqa: F821
schema_prefix = dbutils.widgets.get("schema_prefix")  # type: ignore # noqa: F821
schema = "market_data_and_fundamentals"
tables_to_delete_from = [
    "production_hydro_volue",
    "reservoir_hydro_volue",
]

tables_to_drop = [
    "temperature_river_volue",
]

# COMMAND ----------

for table in tables_to_delete_from:
    table_name = f"{catalog_prefix}silver.{schema_prefix}{schema}.{table}"
    spark.sql(f"DELETE FROM {table_name} WHERE data_type IN ('AF', 'SA')")

for table in tables_to_drop:
    table_name = f"{catalog_prefix}silver.{schema_prefix}{schema}.{table}"
    spark.sql(f"DROP TABLE IF EXISTS {table_name}")
