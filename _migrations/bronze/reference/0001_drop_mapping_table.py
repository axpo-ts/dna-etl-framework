# Databricks notebook source
from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()

# COMMAND ----------


catalog_prefix = dbutils.widgets.get("env_catalog_identifier")  # type: ignore # noqa: F821
schema_prefix = dbutils.widgets.get("schema_prefix")  # type: ignore # noqa: F821

# COMMAND ----------
# clear reference mapping table for new config
spark.sql(f"""
    DROP TABLE IF EXISTS {catalog_prefix}bronze.{schema_prefix}reference.reference_mapping
""")
