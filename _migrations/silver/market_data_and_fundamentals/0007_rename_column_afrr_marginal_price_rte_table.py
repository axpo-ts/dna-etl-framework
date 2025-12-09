# Databricks notebook source
from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()

# COMMAND ----------


catalog_prefix = dbutils.widgets.get("env_catalog_identifier")  # type: ignore # noqa: F821
schema_prefix = dbutils.widgets.get("schema_prefix")  # type: ignore # noqa: F821

# COMMAND ----------
table_name = f"{catalog_prefix}silver.{schema_prefix}market_data_and_fundamentals.afrr_marginal_price_rte"
col_name_old = "identifier"
col_name_new = "id"
if spark.catalog.tableExists(table_name):
    df = spark.table(table_name)
    if col_name_old in df.columns and col_name_new not in df.columns:
        spark.sql(f"ALTER TABLE {table_name} SET TBLPROPERTIES ('delta.columnMapping.mode' = 'name')")  # noqa: E501
        spark.sql(f"ALTER TABLE {table_name} RENAME COLUMN {col_name_old} TO {col_name_new}")  # noqa: E501
