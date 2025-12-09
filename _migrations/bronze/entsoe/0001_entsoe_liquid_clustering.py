# Databricks notebook source

from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()

# COMMAND ----------


catalog_prefix = dbutils.widgets.get("env_catalog_identifier")  # type: ignore # noqa: F821
schema_prefix = dbutils.widgets.get("schema_prefix")  # type: ignore # noqa: F821
schema = "entsoe"


tables = [
    r.tableName
    for r in spark.sql(f"SHOW TABLES IN {catalog_prefix}bronze.{schema_prefix}{schema}").collect()
    if not r.tableName.startswith("_migrations")
]

for t in tables:
    full_table = f"{catalog_prefix}bronze.{schema_prefix}{schema}.{t}"
    print(full_table)
    # If you need to turn off deletion vectors first (only if currently enabled/required):
    spark.sql(f"ALTER TABLE {full_table} SET TBLPROPERTIES ('delta.enableDeletionVectors' = false)")

    # Enable liquid clustering with automatic column selection
    spark.sql(f"ALTER TABLE {full_table} CLUSTER BY AUTO")

    # Optimize the table to incrementally cluster data
    spark.sql(f"OPTIMIZE {full_table}")
