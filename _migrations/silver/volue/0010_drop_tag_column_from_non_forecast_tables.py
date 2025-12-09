# Databricks notebook source

from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()


# COMMAND ----------


catalog_prefix = dbutils.widgets.get("env_catalog_identifier")  # type: ignore # noqa: F821
schema_prefix = dbutils.widgets.get("schema_prefix")  # type: ignore # noqa: F821

schema = "volue"

tables = ["temperature_consumption", "residual_load", "precipitation", "hydro_inflow"]

for table in tables:
    full_table = f"{catalog_prefix}silver.{schema_prefix}{schema}.{table}"
    if spark.catalog.tableExists(full_table):
        spark.sql(f"ALTER TABLE {full_table} SET TBLPROPERTIES ('delta.columnMapping.mode' = 'name')")
        spark.sql(f"ALTER TABLE {full_table} DROP COLUMN IF EXISTS tag")
