# Databricks notebook source

from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()

# COMMAND ----------
# 1. Add column instance_code to silver.entsoe.redispatching_cross_border

catalog_prefix = dbutils.widgets.get("env_catalog_identifier")  # type: ignore # noqa: F821
schema_prefix = dbutils.widgets.get("schema_prefix")  # type: ignore # noqa: F821
schema = "entsoe"
silver_catalog = f"{catalog_prefix}silver"
silver_table = f"{silver_catalog}.{schema}.redispatching_cross_border"

spark.sql(f"""
ALTER TABLE {silver_table}
ADD COLUMN (
    instance_code STRING COMMENT 'A unique code identifying a particular instance' FIRST
)
""")
