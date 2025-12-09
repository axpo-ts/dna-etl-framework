# Databricks notebook source
import json

from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()

# COMMAND ----------


catalog_prefix = dbutils.widgets.get("env_catalog_identifier")  # type: ignore # noqa: F821
schema_prefix = dbutils.widgets.get("schema_prefix")  # type: ignore # noqa: F821
schema = "market_data_and_fundamentals"
tables = ["clear_sky_radiance_meteomatics", "snow_depth_meteomatics", "global_radiance_meteomatics"]


for table in tables:
    full_table = f"{catalog_prefix}silver.{schema_prefix}{schema}.{table}"
    print(f"Table is {full_table}")
    spark.sql(f"DROP TABLE {full_table}")
    spark.sql(f"DROP VIEW {full_table}_view")
