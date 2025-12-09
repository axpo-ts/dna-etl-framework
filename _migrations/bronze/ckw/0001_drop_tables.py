# Databricks notebook source
from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()

# COMMAND ----------


catalog_prefix = dbutils.widgets.get("env_catalog_identifier")  # type: ignore # noqa: F821
schema_prefix = dbutils.widgets.get("schema_prefix")  # type: ignore # noqa: F821
schema = "ckw"
tables = ["tbl_d_anlage", "tbl_d_isu_vertrag"]


# COMMAND ----------

for table in tables:
    table_name = f"{catalog_prefix}bronze.{schema_prefix}{schema}.{table}"

    spark.sql(f"DROP TABLE IF EXISTS {table_name}")
