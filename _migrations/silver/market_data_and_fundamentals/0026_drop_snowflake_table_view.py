# Databricks notebook source
from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()

# COMMAND ----------


catalog_prefix = dbutils.widgets.get("env_catalog_identifier")  # type: ignore # noqa: F821
schema_prefix = dbutils.widgets.get("schema_prefix")  # type: ignore # noqa: F821
schema = "market_data_and_fundamentals"
tables = [
    "peak_offpeak_mapping_endur",
    "market_price_super_index_cmdp",
    "market_price_cmdp",
    "market_price_catalog_cmdp",
]


# COMMAND ----------

for table in tables:
    table_name = f"{catalog_prefix}silver.{schema_prefix}{schema}.{table}"
    view_name = f"{table_name}_view"

    spark.sql(f"DROP TABLE IF EXISTS {table_name}")
    spark.sql(f"DROP VIEW IF EXISTS {view_name}")
