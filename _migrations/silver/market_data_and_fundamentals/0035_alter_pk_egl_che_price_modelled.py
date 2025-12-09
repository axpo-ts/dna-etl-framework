# Databricks notebook source
from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()

# COMMAND ----------


catalog_prefix = dbutils.widgets.get("env_catalog_identifier")  # type: ignore # noqa: F821
schema_prefix = dbutils.widgets.get("schema_prefix")  # type: ignore # noqa: F821
schema = "market_data_and_fundamentals"
table_name = "egl_che_price_modelled"

table_full_name = f"{catalog_prefix}silver.{schema_prefix}{schema}.{table_name}"


# COMMAND ----------

#  Drop the existing primary key constraint
spark.sql(f"ALTER TABLE {table_full_name} DROP CONSTRAINT pk_{table_name}")

# Add a new primary key constraint
spark.sql(
    f"ALTER TABLE {table_full_name} ADD CONSTRAINT pk_{table_name} PRIMARY KEY (`id`, `reference_time`, `delivery_start`, `delivery_end`)"  # noqa: E501
)

spark.sql(f"ALTER TABLE {table_full_name} ALTER COLUMN legacy_delivery_bucket_number DROP NOT NULL")
