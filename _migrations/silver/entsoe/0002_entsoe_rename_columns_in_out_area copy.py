# Databricks notebook source

from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()

# COMMAND ----------


catalog_prefix = dbutils.widgets.get("env_catalog_identifier")  # type: ignore # noqa: F821
schema_prefix = dbutils.widgets.get("schema_prefix")  # type: ignore # noqa: F821
schema = "entsoe"


tables_in_out_map_code = [
    "physical_flow_cross_border",
    "transfer_capacity_net_dayahead_forecast",
    "commercial_exchange_scheduled",
    "cross_border_capacity_dc_link_intraday",
    "nominated_total_capacity",
    "activated_volume_crossborder_balancing_energy",
    "price_crossborder_balancing_energy",
    "offered_volume_crossborder_balancing_energy",
    "transfer_capacity_offered_implicit_dayahead",
    "transfer_capacity_offered_explicit",
    "revenue_auction_explicit_allocation",
    "transfer_capacity_use_explicit_allocation",
]


for table in tables_in_out_map_code:
    table_name = f"{catalog_prefix}silver.{schema_prefix}{schema}.{table}"

    spark.sql(
        f"""
        ALTER TABLE {table_name}
        SET TBLPROPERTIES ('delta.columnMapping.mode' = 'name')
        """
    )
    spark.sql(
        f"""ALTER TABLE {table_name}
        RENAME COLUMN in_map_area_code TO in_area_map_code;"""
    )

    spark.sql(
        f"""ALTER TABLE {table_name}
        RENAME COLUMN out_map_area_code TO out_area_map_code;"""
    )
