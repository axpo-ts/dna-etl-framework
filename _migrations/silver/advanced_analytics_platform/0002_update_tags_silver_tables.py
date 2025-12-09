# Databricks notebook source

from pyspark.sql import SparkSession

from data_platform.data_model.silver.advanced_analytics_platform import (
    attribute_table,
    dayahead_volume_flexpool_table,
    intraday_price_volume_flexpool_table,
)
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
    table_model=dayahead_volume_flexpool_table,
).execute()

UpdateTableMetadata(
    context=context,
    table_model=attribute_table,
).execute()


UpdateTableMetadata(
    context=context,
    table_model=intraday_price_volume_flexpool_table,
).execute()
