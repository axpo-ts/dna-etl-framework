# Databricks notebook source
from databricks.sdk.runtime import dbutils
from pyspark.sql import SparkSession

from data_platform.etl.core.task_context import TaskContext

spark = SparkSession.builder.getOrCreate()

# COMMAND ----------

catalog_prefix = dbutils.widgets.get("env_catalog_identifier")
schema_prefix = dbutils.widgets.get("schema_prefix")

context = TaskContext(
    dbutils=dbutils,
    catalog_prefix=catalog_prefix,
    schema_prefix=schema_prefix,
)

truncate_query = f"""TRUNCATE TABLE {catalog_prefix}bronze.{schema_prefix}jao.auction"""

spark.sql(truncate_query)
