# Databricks notebook source
from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()

# COMMAND ----------


catalog_prefix = dbutils.widgets.get("env_catalog_identifier")  # type: ignore # noqa: F821
schema_prefix = dbutils.widgets.get("schema_prefix")  # type: ignore # noqa: F821
schema = "meteomatics"
tables = [
    "clear_sky_rad_w_mm_euro1k_pt1h",
    "snow_depth_w_mm_euro1k_pt1h",
    "global_rad_w_mm_euro1k_pt1h",
]


# COMMAND ----------

for table in tables:
    table_name = f"{catalog_prefix}bronze.{schema_prefix}{schema}.{table}"
    view_name = f"{table_name}_view"

    spark.sql(f"DROP TABLE IF EXISTS {table_name}")
