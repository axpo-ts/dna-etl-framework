# Databricks notebook source

from pyspark.sql import SparkSession

from data_platform.data_model import StaticTableModel
from data_platform.data_model.silver.volue.attribute import attribute_table
from data_platform.data_model.silver.volue.commercial_exchange_scheduled import (
    commercial_exchange_scheduled_table,
)
from data_platform.data_model.silver.volue.consumption import consumption_table
from data_platform.data_model.silver.volue.consumption_forecast import consumption_forecast_table
from data_platform.data_model.silver.volue.hydro_inflow_forecast import hydro_inflow_forecast_table
from data_platform.data_model.silver.volue.hydro_reservoir import hydro_reservoir_table
from data_platform.data_model.silver.volue.hydro_reservoir_forecast import hydro_reservoir_forecast_table
from data_platform.data_model.silver.volue.precipitation_forecast import precipitation_forecast_table
from data_platform.data_model.silver.volue.price_dayahead import price_dayahead_table
from data_platform.data_model.silver.volue.price_dayahead_forecast import price_dayahead_forecast_table
from data_platform.data_model.silver.volue.price_intraday_auction import price_intraday_auction_table
from data_platform.data_model.silver.volue.production import production_table
from data_platform.data_model.silver.volue.production_forecast import production_forecast_table
from data_platform.data_model.silver.volue.residual_consumption_forecast import (
    residual_consumption_forecast_table,
)
from data_platform.data_model.silver.volue.temperature_consumption_forecast import (
    temperature_consumption_forecast_table,
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
    spark.sql(f"DROP TABLE IF EXISTS {catalog_prefix}silver.{schema_prefix}volue.{table_name}")
    CreateTable(context=context, table_model=table_model).execute()
    return table_name


# create attribute table
create_tables(attribute_table)


forecast_tables = [
    consumption_forecast_table,
    hydro_inflow_forecast_table,
    production_forecast_table,
    hydro_reservoir_forecast_table,
    residual_consumption_forecast_table,
    price_dayahead_forecast_table,
    precipitation_forecast_table,
    temperature_consumption_forecast_table,
]

non_forecast_tables = [
    commercial_exchange_scheduled_table,
    consumption_table,
    production_table,
    hydro_reservoir_table,
    price_dayahead_table,
    price_intraday_auction_table,
]

for table in forecast_tables:
    create_tables(table)

for table in non_forecast_tables:
    create_tables(table)
