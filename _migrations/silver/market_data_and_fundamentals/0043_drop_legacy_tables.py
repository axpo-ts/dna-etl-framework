# Databricks notebook source

from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()


# COMMAND ----------
catalog_prefix = dbutils.widgets.get("env_catalog_identifier")  # type: ignore # noqa: F821
schema_prefix = dbutils.widgets.get("schema_prefix")  # type: ignore # noqa: F821

target_catalog = "silver"
target_schema = "market_data_and_fundamentals"

legacy_tables = [
    {"table_name": "afrr_capacity_price_rte"},
    {"table_name": "afrr_marginal_price_rte"},
    {"table_name": "axpo_trading_fx_forward_curve"},
    {"table_name": "axpo_trading_price_forward_curve"},
    {"table_name": "dayahead_price_epex"},
    {"table_name": "dayahead_volume_flex_epex"},
    {"table_name": "egl_che_cannibalization_factor"},
    {"table_name": "egl_che_discount_factor_curve"},
    {"table_name": "egl_che_price_modelled"},
    {"table_name": "fcr_price_amprion"},
    {"table_name": "imbalance_price_rte"},
    {"table_name": "intraday_price_volume_flex_xbids"},
    {"table_name": "jao_auction"},
    {"table_name": "jao_bids"},
    {"table_name": "montel_contract_price"},
    {"table_name": "montel_trade_price"},
    {"table_name": "nordpool_physical_flow"},
    {"table_name": "nordpool_spot_price"},
    {"table_name": "production_plant_pronovo"},
    {"table_name": "production_spv_meteologica"},
    {"table_name": "production_wind_meteologica"},
    {"table_name": "swissgrid_activation_signal_picasso"},
    {"table_name": "volue_commercial_flow"},
    {"table_name": "volue_consumption"},
    {"table_name": "volue_consumption_forecast"},
    {"table_name": "volue_inflow_hydro_forecast"},
    {"table_name": "volue_precipitation_forecast"},
    {"table_name": "volue_production"},
    {"table_name": "volue_production_forecast"},
    {"table_name": "volue_reservoir_hydro"},
    {"table_name": "volue_reservoir_hydro_forecast"},
    {"table_name": "volue_residual_forecast"},
    {"table_name": "volue_spot_price"},
    {"table_name": "volue_spot_price_forecast"},
]

for table in legacy_tables:
    table_name = table["table_name"]
    full_table_name = f"{catalog_prefix}{target_catalog}.{schema_prefix}{target_schema}.{table_name}"

    print(f"Dropping table if exists: {full_table_name}")
    spark.sql(f"DROP TABLE IF EXISTS {full_table_name}")
