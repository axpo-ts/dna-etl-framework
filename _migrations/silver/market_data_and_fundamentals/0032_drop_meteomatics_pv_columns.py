# Databricks notebook source
from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()

# COMMAND ----------


catalog_prefix = dbutils.widgets.get("env_catalog_identifier")  # type: ignore # noqa: F821
schema_prefix = dbutils.widgets.get("schema_prefix")  # type: ignore # noqa: F821
schema = "market_data_and_fundamentals"
tables = [
    "clear_sky_radiance_meteomatics",
    "snow_depth_meteomatics",
    "global_radiance_meteomatics",
]
columns = [
    "city",
    "level",
    "interval",
    "measure",
]

# COMMAND ----------

for table in tables:
    table_name = f"{catalog_prefix}silver.{schema_prefix}{schema}.{table}"

    for column in columns:
        spark.sql(f"ALTER TABLE {table_name} SET TBLPROPERTIES ('delta.columnMapping.mode' = 'name')")
        spark.sql(f"ALTER TABLE {table_name} DROP COLUMN IF EXISTS {column}")
    spark.sql(f"TRUNCATE TABLE {table_name}")
