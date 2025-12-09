# Databricks notebook source
from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()

# COMMAND ----------


catalog_prefix = dbutils.widgets.get("env_catalog_identifier")  # type: ignore # noqa: F821
schema_prefix = dbutils.widgets.get("schema_prefix")  # type: ignore # noqa: F821

# COMMAND ----------

spark.sql(f"DROP VOLUME IF EXISTS {catalog_prefix}silver.{schema_prefix}deal_data.tds_metadata_volume")

spark.sql(f"DROP TABLE IF EXISTS {catalog_prefix}silver.{schema_prefix}deal_data.endur_power_volume_absolute")

spark.sql(f"DROP VIEW IF EXISTS {catalog_prefix}silver.{schema_prefix}deal_data.endur_power_volume_absolute_view")
