# Databricks notebook source
from databricks.sdk.runtime import dbutils
from pyspark.sql import SparkSession

from data_platform.etl.core.config_loader import ConfigLoader
from data_platform.etl.core.task_context import TaskContext

spark = SparkSession.builder.getOrCreate()

# COMMAND ----------
# Get variables
params = ConfigLoader(
    dbutils=dbutils,
).all()
catalog_prefix: str = params.get("catalog_prefix")
schema_prefix: str = params.get("schema_prefix")

context = TaskContext(catalog_prefix=catalog_prefix, schema_prefix=schema_prefix)

schema = "elviz"

for layer in ["staging", "bronze", "silver", "gold"]:
    schema_name = f"{catalog_prefix}{layer}.{schema_prefix}{schema}"
    context.logger.info(f"Dropping deprecated schema: {schema_name}")
    spark.sql(f"DROP SCHEMA IF EXISTS {schema_name} CASCADE")

context.logger.info("Deprecated schemas removed successfully.")
