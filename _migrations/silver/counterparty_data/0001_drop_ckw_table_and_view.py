# Databricks notebook source
from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()

# COMMAND ----------


catalog_prefix = dbutils.widgets.get("env_catalog_identifier")  # type: ignore # noqa: F821
schema_prefix = dbutils.widgets.get("schema_prefix")  # type: ignore # noqa: F821
schema = "counterparty_data"
tables = ["ckw_asset"]


# COMMAND ----------

for table in tables:
    table_name = f"{catalog_prefix}silver.{schema_prefix}{schema}.{table}"
    view_name = f"{table_name}_view"

    spark.sql(f"DROP TABLE IF EXISTS {table_name}")
    spark.sql(f"DROP VIEW IF EXISTS {view_name}")
