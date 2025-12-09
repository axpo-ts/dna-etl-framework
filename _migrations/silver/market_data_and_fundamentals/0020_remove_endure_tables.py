# Databricks notebook source
from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()

# COMMAND ----------


catalog_prefix = dbutils.widgets.get("env_catalog_identifier")  # type: ignore # noqa: F821
schema_prefix = dbutils.widgets.get("schema_prefix")  # type: ignore # noqa: F821


spark.sql(f"DROP SCHEMA IF EXISTS {catalog_prefix}bronze.{schema_prefix}endur CASCADE")

spark.sql(
    f"DROP TABLE IF EXISTS {catalog_prefix}silver.{schema_prefix}market_data_and_fundamentals.absolute_power_volume_endur"
)

spark.sql(f"DROP TABLE IF EXISTS {catalog_prefix}silver.{schema_prefix}deal_data.absolute_power_volume_endur")
