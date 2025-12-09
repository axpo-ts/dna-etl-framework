# Databricks notebook source

from pyspark.sql import SparkSession

from data_platform.etl.core.task_context import TaskContext


spark = SparkSession.builder.getOrCreate()

# COMMAND ----------

catalog_prefix = dbutils.widgets.get("env_catalog_identifier")  # type: ignore # noqa: F821
schema_prefix = dbutils.widgets.get("schema_prefix")  # type: ignore # noqa: F821

spark.sql(f"DROP TABLE IF EXISTS {catalog_prefix}bronze.{schema_prefix}ebx.legal_entity")
