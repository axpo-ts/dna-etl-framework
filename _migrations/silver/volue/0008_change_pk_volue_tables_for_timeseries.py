# Databricks notebook source
from pyspark.sql import SparkSession

from data_platform.data_model.silver.volue import (
    commercial_exchange_scheduled_table,
    consumption_table,
    hydro_inflow_table,
    hydro_reservoir_table,
    precipitation_table,
    price_dayahead_table,
    price_intraday_auction_table,
    price_intraday_continuous_table,
    production_table,
    residual_load_table,
    temperature_consumption_table,
)

spark = SparkSession.builder.getOrCreate()


# COMMAND ----------


catalog_prefix = dbutils.widgets.get("env_catalog_identifier")  # type: ignore # noqa: F821
schema_prefix = dbutils.widgets.get("schema_prefix")  # type: ignore # noqa: F821
schema = "volue"


for table in [
    commercial_exchange_scheduled_table,
    consumption_table,
    hydro_inflow_table,
    hydro_reservoir_table,
    precipitation_table,
    price_dayahead_table,
    price_intraday_auction_table,
    price_intraday_continuous_table,
    production_table,
    residual_load_table,
    temperature_consumption_table,
]:
    primary_keys = table.primary_keys

    for col in primary_keys:
        spark.sql(
            f"ALTER TABLE {catalog_prefix}silver.{schema_prefix}{schema}.{table.identifier.name} ALTER COLUMN {col} SET NOT NULL"  # noqa: E501
        )
    spark.sql(
        f"ALTER TABLE {catalog_prefix}silver.{schema_prefix}{schema}.{table.identifier.name} DROP PRIMARY KEY IF EXISTS"
    )
    pk_string = ", ".join(primary_keys)
    spark.sql(
        f"ALTER TABLE {catalog_prefix}silver.{schema_prefix}{schema}.{table.identifier.name} ADD PRIMARY KEY ({pk_string})"  # noqa: E501
    )
