# Databricks notebook source
from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()

# COMMAND ----------


catalog_prefix = dbutils.widgets.get("env_catalog_identifier")  # type: ignore # noqa: F821
schema_prefix = dbutils.widgets.get("schema_prefix")  # type: ignore # noqa: F821

# COMMAND ----------
old_table = f"{catalog_prefix}silver.{schema_prefix}market_data_and_fundamentals.da_prices_epex"
new_table = f"{catalog_prefix}silver.{schema_prefix}market_data_and_fundamentals.dayahead_price_epex"
if spark.catalog.tableExists(old_table):
    spark.sql(f"ALTER TABLE {old_table} RENAME TO {new_table}")
