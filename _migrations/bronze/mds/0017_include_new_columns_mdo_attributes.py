# Databricks notebook source
from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()

# COMMAND ----------
catalog_prefix = dbutils.widgets.get("env_catalog_identifier")  # type: ignore # noqa: F821
schema_prefix = dbutils.widgets.get("schema_prefix")  # type: ignore # noqa: F821
table_name_target = f"{catalog_prefix}bronze.{schema_prefix}mds.mdo_attributes"

# COMMAND ----------
if spark.catalog.tableExists(table_name_target):
    # List of new columns to be created
    new_columns = [
        ("Contract_Profile", "STRING"),
        ("Contract_Session", "STRING"),
        ("Contract_MarketMechanism", "STRING"),
        ("Contract_Settlement", "STRING"),
    ]

    # Get existing columns from the table
    existing_columns = [col.name for col in spark.table(table_name_target).schema.fields]

    # Add new columns, if they do not exist (and populate with nulls)
    for name, type in reversed(new_columns):
        if name not in existing_columns:
            print(f"Adding column {name} of type {type} to table {table_name_target}")
            spark.sql(f"""
                ALTER TABLE {table_name_target}
                ADD COLUMNS ({name} {type} FIRST)
            """)
