# Databricks notebook source
from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()

# COMMAND ----------
# Get variables
catalog_prefix = dbutils.widgets.get("env_catalog_identifier")  # type: ignore # noqa: F821
schema_prefix = dbutils.widgets.get("schema_prefix")  # type: ignore # noqa: F821

# Set script parameters
catalog = "silver"
schema = "volue"
table_name = "attribute"

# COMMAND ---------- Compose target table string
target_table = f"{catalog_prefix}{catalog}.{schema_prefix}{schema}.{table_name}"


# COMMAND ---------- Check table and columns
spark.sql(f"""
    ALTER TABLE {target_table}
    ADD COLUMNS (silver_table STRING COMMENT 'Contains the table name where the curve id is stored in the silver catalog' AFTER is_current)
""")  # noqa: E501
