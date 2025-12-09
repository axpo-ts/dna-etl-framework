# Databricks notebook source
from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()

# COMMAND ----------


catalog_prefix = dbutils.widgets.get("env_catalog_identifier")  # type: ignore # noqa: F821
schema_prefix = dbutils.widgets.get("schema_prefix")  # type: ignore # noqa: F821
schema = "data_quality"
table = "data_quality_rules"


spark.sql(f"DROP TABLE IF EXISTS {catalog_prefix}bronze.{schema_prefix}{schema}.{table}")
spark.sql(f"DROP TABLE IF EXISTS {catalog_prefix}silver.{schema_prefix}{schema}.{table}")
spark.sql(f"DROP TABLE IF EXISTS {catalog_prefix}gold.{schema_prefix}{schema}.{table}")
