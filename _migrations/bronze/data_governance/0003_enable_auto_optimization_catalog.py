# Databricks notebook source
from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()

# COMMAND ----------


catalog_prefix = dbutils.widgets.get("env_catalog_identifier")  # type: ignore # noqa: F821
catalog_name = ["staging", "bronze", "silver", "gold"]

# COMMAND ----------
for catalog in catalog_name:
    name = f"{catalog_prefix}{catalog}"
    spark.sql(f"ALTER CATALOG {name} ENABLE PREDICTIVE OPTIMIZATION;")
