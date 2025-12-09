# Databricks notebook source

from pyspark.sql import SparkSession

from data_platform.data_model.bronze.ebx import (
    commodity_table,
    country_table,
    currency_table,
    language_table,
    unit_table,
)
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

spark.sql(f"DROP TABLE IF EXISTS {context.full_unitycatalog_name(commodity_table.identifier)}")
spark.sql(f"DROP TABLE IF EXISTS {context.full_unitycatalog_name(country_table.identifier)}")
spark.sql(f"DROP TABLE IF EXISTS {context.full_unitycatalog_name(currency_table.identifier)}")
spark.sql(f"DROP TABLE IF EXISTS {context.full_unitycatalog_name(language_table.identifier)}")
spark.sql(f"DROP TABLE IF EXISTS {context.full_unitycatalog_name(unit_table.identifier)}")

CreateTable(context=context, table_model=commodity_table).execute()
CreateTable(context=context, table_model=country_table).execute()
CreateTable(context=context, table_model=currency_table).execute()
CreateTable(context=context, table_model=language_table).execute()
CreateTable(context=context, table_model=unit_table).execute()
