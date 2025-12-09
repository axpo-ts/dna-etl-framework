# Databricks notebook source
from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()

# COMMAND ----------


catalog_prefix = dbutils.widgets.get("env_catalog_identifier")  # type: ignore # noqa: F821
schema_prefix = dbutils.widgets.get("schema_prefix")  # type: ignore # noqa: F821
schema_name = f"{catalog_prefix}gold.{schema_prefix}pv_forecast"

spark.sql(
    "CREATE FUNCTION IF NOT EXISTS "
    f"{schema_name}.fx_row_filter_license (license STRING) "
    "RETURNS BOOLEAN RETURN ("
    "IS_ACCOUNT_GROUP_MEMBER('admins') OR "
    "IS_ACCOUNT_GROUP_MEMBER('dna_admins') OR "
    "is_member(license))"
)
