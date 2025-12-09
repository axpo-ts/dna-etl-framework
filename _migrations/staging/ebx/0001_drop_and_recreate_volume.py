# Databricks notebook source
from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()

# COMMAND ----------

catalog_prefix = dbutils.widgets.get("env_catalog_identifier")  # type: ignore # noqa: F821
schema_prefix = dbutils.widgets.get("schema_prefix")  # type: ignore # noqa: F821

# COMMAND ----------
schema_name = f"{catalog_prefix}staging.{schema_prefix}ebx"
volume_name = "commodity".lower()
spark.sql(f"DROP VOLUME IF EXISTS {schema_name}.{volume_name}")
spark.sql(f"CREATE VOLUME {schema_name}.{volume_name}")
