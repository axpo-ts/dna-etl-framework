# Databricks notebook source
from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()

# COMMAND ----------


catalog_prefix = dbutils.widgets.get("env_catalog_identifier")  # type: ignore # noqa: F821
schema_prefix = dbutils.widgets.get("schema_prefix")  # type: ignore # noqa: F821
old_table = f"{catalog_prefix}silver.{schema_prefix}market_data_and_fundamentals.pronovo_production_plants"
new_table = f"{catalog_prefix}silver.{schema_prefix}market_data_and_fundamentals.production_plant_pronovo"
old_view = f"{catalog_prefix}silver.{schema_prefix}market_data_and_fundamentals.production_plant_pronovo_view"
# COMMAND ----------
if spark.catalog.tableExists(f"{old_table}"):
    spark.sql(f"DROP TABLE IF EXISTS {old_view}")

if spark.catalog.tableExists(old_table):
    spark.sql(f"ALTER TABLE {old_table} RENAME TO {new_table}")
