# Databricks notebook source

from pyspark.sql import SparkSession

from data_platform.data_model import StaticTableModel
from data_platform.data_model.silver.volue import (
    price_intraday_continuous_table,
    residual_load_forecast_table,
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

# remove deprecated tables

spark.sql(f"DROP TABLE IF EXISTS {catalog_prefix}silver.{schema_prefix}volue.price_order_intraday_continuous")
spark.sql(f"DROP TABLE IF EXISTS {catalog_prefix}silver.{schema_prefix}volue.residual_consumption_forecast")


def create_tables(table_model: StaticTableModel) -> str:  # noqa: D103
    table_name = table_model.identifier.name
    spark.sql(f"DROP TABLE IF EXISTS {catalog_prefix}silver.{schema_prefix}volue.{table_name}")
    CreateTable(context=context, table_model=table_model).execute()
    return table_name


rename_tables = [price_intraday_continuous_table, residual_load_forecast_table]

for table in rename_tables:
    create_tables(table)
