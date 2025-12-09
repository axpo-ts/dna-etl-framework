# Databricks notebook source
from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()

# COMMAND ----------


catalog_prefix = dbutils.widgets.get("env_catalog_identifier")  # type: ignore # noqa: F821
schema_prefix = dbutils.widgets.get("schema_prefix")  # type: ignore # noqa: F821

schema_name = f"{catalog_prefix}gold.{schema_prefix}weather"

spark.sql(f"CREATE FUNCTION IF NOT EXISTS {schema_name}.filter_lpi(license STRING) RETURN is_member(license)")
