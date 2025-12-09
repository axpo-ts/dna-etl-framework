# Databricks notebook source

from pyspark.sql import SparkSession

from data_platform.data_model.silver.orca_fr import attribute_table, flexpool_france_monitoring_table
from data_platform.etl.core.task_context import TaskContext
from data_platform.etl.load.create_table import CreateTable

spark = SparkSession.builder.getOrCreate()

# COMMAND ----------


catalog_prefix = dbutils.widgets.get("env_catalog_identifier")  # type: ignore # noqa: F821
schema_prefix = dbutils.widgets.get("schema_prefix")  # type: ignore # noqa: F821

context = TaskContext(
    dbutils=dbutils,  # noqa: F821
    catalog_prefix=catalog_prefix,
    schema_prefix=schema_prefix,
)

spark.sql(f"DROP TABLE IF EXISTS {context.full_unitycatalog_name(attribute_table.identifier)}")
spark.sql(f"DROP TABLE IF EXISTS {context.full_unitycatalog_name(flexpool_france_monitoring_table.identifier)}")

CreateTable(context=context, table_model=attribute_table).execute()
CreateTable(context=context, table_model=flexpool_france_monitoring_table).execute()
