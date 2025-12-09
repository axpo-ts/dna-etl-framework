# Databricks notebook source

from pyspark.sql import SparkSession

from data_platform.data_model.silver.ckw import asset_table, attribute_table, contract_table
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

spark.sql(f"DROP TABLE IF EXISTS {context.full_unitycatalog_name(asset_table.identifier)}")
spark.sql(f"DROP TABLE IF EXISTS {context.full_unitycatalog_name(attribute_table.identifier)}")
spark.sql(f"DROP TABLE IF EXISTS {context.full_unitycatalog_name(contract_table.identifier)}")

CreateTable(context=context, table_model=asset_table).execute()
CreateTable(context=context, table_model=attribute_table).execute()
CreateTable(context=context, table_model=contract_table).execute()
