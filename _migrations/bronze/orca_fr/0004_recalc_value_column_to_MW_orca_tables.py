# Databricks notebook source
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit

spark = SparkSession.builder.getOrCreate()

# COMMAND ----------


catalog_prefix = dbutils.widgets.get("env_catalog_identifier")  # type: ignore # noqa: F821
schema_prefix = dbutils.widgets.get("schema_prefix")  # type: ignore # noqa: F821

tables_and_schemas = [
    {"table_name": "fcr_pos_awarded_capacity_power_orca", "schema_name": "orca_fr"},
    {"table_name": "fcr_neg_awarded_capacity_power_orca", "schema_name": "orca_fr"},
    {"table_name": "afrr_neg_awarded_capacity_power_orca", "schema_name": "orca_fr"},
    {"table_name": "afrr_pos_awarded_capacity_power_orca", "schema_name": "orca_fr"},
    {"table_name": "fcr_reaction_power_orca", "schema_name": "orca_fr"},
    {"table_name": "afrr_reaction_power_orca", "schema_name": "orca_fr"},
    {"table_name": "afrr_request_power_orca", "schema_name": "orca_fr"},
    {"table_name": "schedule_power_orca", "schema_name": "orca_fr"},
    {"table_name": "effective_power_orca", "schema_name": "orca_fr"},
    {"table_name": "realised_schedule_power_orca", "schema_name": "orca_fr"},
]

for table_info in tables_and_schemas:
    table_name_full = f"{catalog_prefix}bronze.{schema_prefix}{table_info['schema_name']}.{table_info['table_name']}"
    print(f"Processing table {table_name_full}")
    schema_name = table_info["schema_name"]
    print(f"Processing table {table_name_full}")
    if spark.catalog.tableExists(table_name_full):
        print(f"Table {table_name_full} already exists")
        df = spark.read.table(table_name_full)

        if "value" in df.columns:
            # Drop existing table
            sql_query = f"UPDATE {table_name_full} SET value = value / 1000"
            spark.sql(sql_query)
