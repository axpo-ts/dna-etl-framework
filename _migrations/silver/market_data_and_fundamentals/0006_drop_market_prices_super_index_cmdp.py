# Databricks notebook source
from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()

# COMMAND ----------


catalog_prefix = dbutils.widgets.get("env_catalog_identifier")  # type: ignore # noqa: F821
schema_prefix = dbutils.widgets.get("schema_prefix")  # type: ignore # noqa: F821

# COMMAND ----------

spark.sql(
    f"DROP TABLE IF EXISTS {catalog_prefix}silver.{schema_prefix}market_data_and_fundamentals.market_prices_super_index_cmdp"
)
