# Databricks notebook source
from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()

# COMMAND ----------


catalog_prefix = dbutils.widgets.get("env_catalog_identifier")  # type: ignore # noqa: F821
schema_prefix = dbutils.widgets.get("schema_prefix")  # type: ignore # noqa: F821
schema = "market_data_and_fundamentals"
table_name = "activation_signal_picasso_swissgrid"

table_full_name = f"{catalog_prefix}silver.{schema_prefix}{schema}.{table_name}"


# COMMAND ----------

#  Delete data from the table
spark.sql(f"DELETE FROM {table_full_name}")
