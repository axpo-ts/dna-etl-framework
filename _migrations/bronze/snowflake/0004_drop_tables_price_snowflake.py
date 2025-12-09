# Databricks notebook source
from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()

# COMMAND ----------


catalog_prefix = dbutils.widgets.get("env_catalog_identifier")  # type: ignore # noqa: F821
schema_prefix = dbutils.widgets.get("schema_prefix")  # type: ignore # noqa: F821
schema = "snowflake"


# COMMAND ----------
tables = [
    "price_forward_curves_prices_latest",
    "price_forward_curves_latest",
    "price_forward_curves_super_index_prices_latest",
]
for table in tables:
    table_name = f"{catalog_prefix}bronze.{schema_prefix}{schema}.{table}"
    if spark.catalog.tableExists(table_name):
        spark.sql(f"DROP TABLE {table_name}")
