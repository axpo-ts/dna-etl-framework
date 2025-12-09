# Databricks notebook source
from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()


# COMMAND ----------


catalog_prefix = dbutils.widgets.get("env_catalog_identifier")  # type: ignore # noqa: F821
schema_prefix = dbutils.widgets.get("schema_prefix")  # type: ignore # noqa: F821
schema = "volue"

spark.sql(
    f"ALTER TABLE {catalog_prefix}silver.{schema_prefix}{schema}.attribute SET TBLPROPERTIES ('delta.columnMapping.mode' = 'name')"  # noqa: E501
)
spark.sql(f"ALTER TABLE {catalog_prefix}silver.{schema_prefix}{schema}.attribute DROP COLUMN has_access")
spark.sql(f"ALTER TABLE {catalog_prefix}silver.{schema_prefix}{schema}.attribute DROP COLUMN access_range_begin")
spark.sql(f"ALTER TABLE {catalog_prefix}silver.{schema_prefix}{schema}.attribute DROP COLUMN access_range_end")
