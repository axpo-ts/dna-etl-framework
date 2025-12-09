# Databricks notebook source
from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()

# COMMAND ----------


catalog_prefix = dbutils.widgets.get("env_catalog_identifier")  # type: ignore # noqa: F821
schema_prefix = dbutils.widgets.get("schema_prefix")  # type: ignore # noqa: F821
schema = "mds"
tables = [
    "price_forward_curve",
]
columns = [
    "value",
]

# COMMAND ----------

for table in tables:
    table_name = f"{catalog_prefix}silver.{schema_prefix}{schema}.{table}"
    print(f"Altering table: {table_name}")
    spark.sql(f"ALTER TABLE {table_name} SET TBLPROPERTIES ('delta.columnMapping.mode' = 'name')")

    for column in columns:
        spark.sql(f"ALTER TABLE {table_name} DROP COLUMN IF EXISTS {column}")
