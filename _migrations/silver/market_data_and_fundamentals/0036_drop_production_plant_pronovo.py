# Databricks notebook source
from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()

# COMMAND ----------


catalog_prefix = dbutils.widgets.get("env_catalog_identifier")  # type: ignore # noqa: F821
schema_prefix = dbutils.widgets.get("schema_prefix")  # type: ignore # noqa: F821
schema = "market_data_and_fundamentals"
table_name = "production_plant_pronovo"

table_full_name = f"{catalog_prefix}silver.{schema_prefix}{schema}.{table_name}"


# COMMAND ----------

#  drop table to remove duplicates, update with new license and type 2 columns
spark.sql(f"drop table if exists {table_full_name}")
