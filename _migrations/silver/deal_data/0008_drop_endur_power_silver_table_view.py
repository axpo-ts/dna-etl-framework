# Databricks notebook source
from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()

# COMMAND ----------


catalog_prefix = dbutils.widgets.get("env_catalog_identifier")  # type: ignore # noqa: F821
schema_prefix = dbutils.widgets.get("schema_prefix")  # type: ignore # noqa: F821
bronze_schema = "tds"
silver_schema = "deal_data"
tables = ["endur_power_volume_delta"]


# COMMAND ----------

for table in tables:
    full_table_name = f"{catalog_prefix}silver.{schema_prefix}{silver_schema}.{table}"
    view_name = f"{full_table_name}_view"

    spark.sql(f"DROP TABLE IF EXISTS {full_table_name}")
    spark.sql(f"DROP VIEW IF EXISTS {view_name}")

    spark.sql(f"drop volume {catalog_prefix}bronze.{schema_prefix}{bronze_schema}.{table}")
    spark.sql(f"drop volume {catalog_prefix}silver.{schema_prefix}{silver_schema}.{table}")

    spark.sql(f"create volume {catalog_prefix}bronze.{schema_prefix}{bronze_schema}.{table}")
    spark.sql(f"create volume {catalog_prefix}silver.{schema_prefix}{silver_schema}.{table}")
