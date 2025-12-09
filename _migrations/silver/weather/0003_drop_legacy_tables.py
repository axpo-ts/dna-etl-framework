# Databricks notebook source

from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()


# COMMAND ----------
catalog_prefix = dbutils.widgets.get("env_catalog_identifier")  # type: ignore # noqa: F821
schema_prefix = dbutils.widgets.get("schema_prefix")  # type: ignore # noqa: F821

target_catalog = "silver"
target_schema = "weather"

legacy_tables = [
    {"table_name": "meteomatics_actual"},
    {"table_name": "meteomatics_forecast"},
    {"table_name": "noaa_precipitation"},
    {"table_name": "volue_precipitation_forecast"},
    {"table_name": "volue_temperature_consumption_forecast"},
]

for table in legacy_tables:
    table_name = table["table_name"]
    full_table_name = f"{catalog_prefix}{target_catalog}.{schema_prefix}{target_schema}.{table_name}"

    print(f"Dropping table if exists: {full_table_name}")
    spark.sql(f"DROP TABLE IF EXISTS {full_table_name}")
