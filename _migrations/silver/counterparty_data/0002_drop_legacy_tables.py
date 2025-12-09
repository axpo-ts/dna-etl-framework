# Databricks notebook source

from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()


# COMMAND ----------
catalog_prefix = dbutils.widgets.get("env_catalog_identifier")  # type: ignore # noqa: F821
schema_prefix = dbutils.widgets.get("schema_prefix")  # type: ignore # noqa: F821

target_catalog = "silver"
target_schema = "counterparty_data"

legacy_tables = [
    {"table_name": "ckw_asset"},
    {"table_name": "book_ebx"},
    {"table_name": "legal_entity_ebx"},
    {"table_name": "business_unit_ebx"},
    {"table_name": "customergroup_ebx"},
]

for table in legacy_tables:
    table_name = table["table_name"]
    full_table_name = f"{catalog_prefix}{target_catalog}.{schema_prefix}{target_schema}.{table_name}"

    print(f"Dropping table if exists: {full_table_name}")
    spark.sql(f"DROP TABLE IF EXISTS {full_table_name}")
