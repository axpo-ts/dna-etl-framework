# Databricks notebook source
from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()

# COMMAND ----------

catalog_prefix = dbutils.widgets.get("env_catalog_identifier")  # type: ignore # noqa: F821
schema_prefix = dbutils.widgets.get("schema_prefix")  # type: ignore # noqa: F821

table_suffixes = ["nordpool_physical_flow", "nordpool_flow"]
columns_to_rename = {"delivery_area": "from_area", "target_area": "to_area"}

for suffix in table_suffixes:
    table_name = f"{catalog_prefix}silver.{schema_prefix}market_data_and_fundamentals.{suffix}"
    if spark.catalog.tableExists(table_name):
        df = spark.table(table_name)
        existing_renames = {col: new for col, new in columns_to_rename.items() if col in df.columns}
        if existing_renames:
            spark.sql(f"ALTER TABLE {table_name} SET TBLPROPERTIES ('delta.columnMapping.mode' = 'name')")
            for old_col, new_col in existing_renames.items():
                spark.sql(f"ALTER TABLE {table_name} RENAME COLUMN {old_col} TO {new_col}")
