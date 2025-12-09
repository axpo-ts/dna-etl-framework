# Databricks notebook source
from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()

# COMMAND ----------


catalog_prefix = dbutils.widgets.get("env_catalog_identifier")  # type: ignore # noqa: F821
schema_prefix = dbutils.widgets.get("schema_prefix")  # type: ignore # noqa: F821

table_name = f"{catalog_prefix}silver.{schema_prefix}market_data_and_fundamentals.geopotential_height_meteomatics"
column_name = "pressure_density"
if spark.catalog.tableExists(table_name):
    df = spark.table(table_name)
    if column_name in df.columns:
        spark.sql(f"ALTER TABLE {table_name} SET TBLPROPERTIES ('delta.columnMapping.mode' = 'name')")  # noqa: E501

        spark.sql(f"ALTER TABLE {table_name} RENAME COLUMN {column_name} TO geopotential_height")  # noqa: E501
