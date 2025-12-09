# Databricks notebook source

from databricks.sdk.runtime import dbutils
from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()

# COMMAND ----------

catalog_prefix = dbutils.widgets.get("env_catalog_identifier")  # type: ignore
schema_prefix = dbutils.widgets.get("schema_prefix")  # type: ignore

for table in [
    "book",
    "business_unit",
    "customer_group",
    "legal_entity",
]:
    full_table_name = f"{catalog_prefix}silver.{schema_prefix}ebx.{table}"

    spark.sql(f"DROP TABLE IF EXISTS {full_table_name}")


for references_tables in [
    "country_ext",
    "unit_type",
]:
    full_table_name = f"{catalog_prefix}silver.{schema_prefix}reference.{references_tables}"

    spark.sql(f"DROP TABLE IF EXISTS {full_table_name}")
