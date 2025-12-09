# Databricks notebook source

from pyspark.sql import SparkSession

from data_platform.data_model import StaticTableModel
from data_platform.data_model.silver.volue.physical_flow_exchange import physical_flow_exchange_table
from data_platform.data_model.silver.volue.power_transfer_distribution_factor_exchange import (
    power_transfer_distribution_factor_exchange_table,
)
from data_platform.data_model.silver.volue.power_transfer_distribution_factor_exchange_forecast import (
    power_transfer_distribution_factor_exchange_forecast_table,
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
    CreateTable(context=context, table_model=table_model).execute()
    return table_name


# create physical_flow_exchange_table table
create_tables(physical_flow_exchange_table)
# create power_transfer_distribution_factor_exchange_table table
create_tables(power_transfer_distribution_factor_exchange_table)
# create power_transfer_distribution_factor_exchange_table table
create_tables(power_transfer_distribution_factor_exchange_forecast_table)
