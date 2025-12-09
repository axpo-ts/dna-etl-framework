# Databricks notebook source
from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()

# COMMAND ----------


catalog_prefix = dbutils.widgets.get("env_catalog_identifier")  # type: ignore # noqa: F821
schema_prefix = dbutils.widgets.get("schema_prefix")  # type: ignore # noqa: F821
schema = "weather"
tables = [
    "production_hydro_volue_climatology",
    "production_spv_meteologica_climatology",
    "production_spv_meteologica_volue",
    "production_spv_meteologica_volue_climatology",
    "production_spv_volue_climatology",
    "production_wind_meteologica_climatology",
    "production_wind_meteologica_volue",
    "production_wind_meteologica_volue_climatology",
    "production_wind_volue_climatology",
]


# COMMAND ----------

for table in tables:
    table_name = f"{catalog_prefix}gold.{schema_prefix}{schema}.{table}"

    spark.sql(f"DROP TABLE IF EXISTS {table_name}")
