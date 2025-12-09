# Databricks notebook source

from pyspark.sql import SparkSession

from data_platform.data_model.silver.geocat import production_plant_table
from data_platform.etl.core.task_context import TaskContext
from data_platform.etl.load.update_table_metadata import UpdateTableMetadata

spark = SparkSession.builder.getOrCreate()

# COMMAND ----------


catalog_prefix = dbutils.widgets.get("env_catalog_identifier")  # type: ignore # noqa: F821
schema_prefix = dbutils.widgets.get("schema_prefix")  # type: ignore # noqa: F821

context = TaskContext(
    dbutils=dbutils,  # noqa: F821
    catalog_prefix=catalog_prefix,
    schema_prefix=schema_prefix,
)


UpdateTableMetadata(
    context=context,
    table_model=production_plant_table,
).execute()
