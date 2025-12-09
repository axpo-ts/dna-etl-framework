# Databricks notebook source
from pyspark.sql import DataFrame, SparkSession

"""
The purpose of this migration script is to: is to rename these volue silver tables (timeseries):
1. reservoir_hydro_volue -> volue_reservoir_hydro
2. consumption_volue -> volue_consumption
3. day_ahead_commercial_flow_volue -> volue_commercial_flow
4. spot_price_volue -> volue_spot_price
5. temperature_river_volue -> volue_temperature

# And then rename these volue silver forecast tables (instances):
1. reservoir_hydro_forecast_volue -> volue_reservoir_hydro_forecast #
2. spot_price_forecast_volue -> volue_spot_price_forecast #
3. consumption_forecast_volue -> volue_consumption_forecast #

# Rename thse volue silver forecast tables (tagged instances)
1. spot_price_tagged_forecast_volue -> volue_spot_price_tagged_forecast # VERIFY
2. temperature_tagged_forecast_volue -> volue_temperature_tagged_forecast
3. production_tagged_forecast_volue -> volue_production_tagged_forecast
4. precipitation_tagged_forecast_volue -> volue_precipitation_tagged_forecast
5. consumption_tagged_forecast_volue -> volue_consumption_tagged_forecast

# And then consolidate these tables (timeseries)
1. wind_production_volue, spv_production_volue, hydro_production_volue -> volue_production
2. production_wind_forecast_volue, production_spv_forecast_volue -> volue_production_forecast
"""


spark = SparkSession.builder.getOrCreate()
catalog_prefix = dbutils.widgets.get("env_catalog_identifier")  # type: ignore # noqa: F821
schema_prefix = dbutils.widgets.get("schema_prefix")  # type: ignore # noqa: F821
# COMMAND ----------

base_path = f"{catalog_prefix}silver.{schema_prefix}market_data_and_fundamentals"

# Table rename mapping
rename_mapping = {
    # timeseries
    "reservoir_hydro_volue": "volue_reservoir_hydro",
    "consumption_volue": "volue_consumption",
    "day_ahead_commercial_flow_volue": "volue_commercial_flow",
    "spot_price_volue": "volue_spot_price",
    "temperature_river_volue": "volue_temperature",
    # instances
    "reservoir_hydro_forecast_volue": "volue_reservoir_hydro_forecast",
    "spot_price_forecast_volue": "volue_spot_price_forecast",
    "temperature_consumption_forecast_volue": "volue_temperature_forecast",
    "consumption_forecast_volue": "volue_consumption_forecast",
    # tagged instances
    "spot_price_tagged_forecast_volue": "volue_spot_price_tagged_forecast",
    "temperature_tagged_forecast_volue": "volue_temperature_tagged_forecast",
    "production_tagged_forecast_volue": "volue_production_tagged_forecast",
    "precipitation_tagged_forecast_volue": "volue_precipitation_tagged_forecast",
    "consumption_tagged_forecast_volue": "volue_consumption_tagged_forecast",
}

for old_table, new_table in rename_mapping.items():
    full_old = f"{base_path}.{old_table}"
    full_new = f"{base_path}.{new_table}"
    if spark.catalog.tableExists(full_old):
        spark.sql(f"ALTER TABLE {full_old} RENAME TO {full_new}")

# Join production tables
prod_tables = [
    f"{base_path}.wind_production_volue",
    f"{base_path}.spv_production_volue",
    f"{base_path}.hydro_production_volue",
]

# instances, new additions from PR #801 joined
prod_forecast_tables = [
    f"{base_path}.production_wind_forecast_volue",
    f"{base_path}.production_spv_forecast_volue",
]


def join_prod_tables(tables: list[str]) -> DataFrame:
    """Join multiple production tables into a single DataFrame."""
    existing_tables = [tbl for tbl in tables if spark.catalog.tableExists(tbl)]
    if not existing_tables:
        return None

    dfs = [spark.table(tbl) for tbl in existing_tables]
    prod_df = dfs[0]
    for df in dfs[1:]:
        prod_df = prod_df.unionByName(df, allowMissingColumns=True)

    return prod_df


existing_prod_tables = [tbl for tbl in prod_tables if spark.catalog.tableExists(tbl)]
if existing_prod_tables:
    prod_df = join_prod_tables(existing_prod_tables)
    if prod_df is not None:
        output_table = f"{base_path}.volue_production"
        prod_df.write.format("delta").mode("overwrite").saveAsTable(output_table)

existing_prod_forecast_tables = [tbl for tbl in prod_forecast_tables if spark.catalog.tableExists(tbl)]
if existing_prod_forecast_tables:
    prod_forecast_df = join_prod_tables(existing_prod_forecast_tables)
    if prod_forecast_df is not None:
        output_forecast_table = f"{base_path}.volue_production_forecast"
        prod_forecast_df.write.format("delta").mode("overwrite").saveAsTable(output_forecast_table)
