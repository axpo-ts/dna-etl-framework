# Databricks notebook source

from pyspark.sql import SparkSession

from data_platform.data_model import StaticTableModel
from data_platform.data_model.bronze.swissgrid import (
    activation_signal_table,
    afrr_activations_table,
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


def create_tables(table_model: StaticTableModel) -> str:  # noqa: D103
    table_name = table_model.identifier.name
    spark.sql(f"DROP TABLE IF EXISTS {catalog_prefix}bronze.{schema_prefix}swissgrid.{table_name}")
    CreateTable(context=context, table_model=table_model).execute()
    return table_name


tables = [
    activation_signal_table,
    afrr_activations_table,
]

for table in tables:
    create_tables(table)

# COMMAND ----------

# Drop deprecreated bronze attributes table

spark.sql(f"DROP TABLE IF EXISTS {catalog_prefix}bronze.{schema_prefix}swissgrid.attributes")
