# Databricks notebook source

from pyspark.sql import SparkSession

from data_platform.data_model import StaticTableModel
from data_platform.data_model.silver.mds import (
    attribute_table,
    curve_fx_forward_table,
    factor_cannibalization_table,
    factor_discount_table,
    precipitation_table,
    price_afrr_capacity_table,
    price_afrr_energy_table,
    price_balancing_energy_table,
    price_dayahead_table,
    price_end_of_day_settlement_table,
    price_fcr_capacity_table,
    price_forward_curve_table,
    production_reanalysis_table,
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
    spark.sql(f"DROP TABLE IF EXISTS {catalog_prefix}silver.{schema_prefix}mds.{table_name}")
    CreateTable(context=context, table_model=table_model).execute()
    return table_name


tables = [
    attribute_table,
    curve_fx_forward_table,
    factor_cannibalization_table,
    factor_discount_table,
    precipitation_table,
    price_afrr_capacity_table,
    price_afrr_energy_table,
    price_balancing_energy_table,
    price_dayahead_table,
    price_end_of_day_settlement_table,
    price_fcr_capacity_table,
    price_forward_curve_table,
    production_reanalysis_table,
]

for table in tables:
    create_tables(table)
