# Databricks notebook source
from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()

# COMMAND ----------


catalog_prefix = dbutils.widgets.get("env_catalog_identifier")  # type: ignore # noqa: F821
schema_prefix = dbutils.widgets.get("schema_prefix")  # type: ignore # noqa: F821
schema = "market_data_and_fundamentals"
tables = [
    "fcr_price_amprion",
    "afrr_capacity_price_rte",
    "afrr_marginal_price_rte",
    "dayahead_price_epex",
    "imbalance_price_rte",
    "precipitation_noaa",
    "production_wind_meteologica",
    "production_spv_meteologica",
    "axpo_trading_fx_forward_curve",
    "egl_che_discount_factor_curve",
    "egl_che_cannibalization_factor",
    "egl_che_price_modelled",
    "axpo_trading_price_forward_curve",
]


# COMMAND ----------

for table in tables:
    table_name = f"{catalog_prefix}silver.{schema_prefix}{schema}.{table}"
    view_name = f"{table_name}_view"

    spark.sql(f"DROP TABLE IF EXISTS {table_name}")
    spark.sql(f"DROP VIEW IF EXISTS {view_name}")
