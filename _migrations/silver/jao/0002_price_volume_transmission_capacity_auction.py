# Databricks notebook source

from pyspark.sql import SparkSession

from data_platform.data_model.silver.jao import price_volume_transmission_capacity_auction_table
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


CreateTable(context=context, table_model=price_volume_transmission_capacity_auction_table).execute()

load_query = f"""INSERT INTO {catalog_prefix}silver.{schema_prefix}jao.price_volume_transmission_capacity_auction
SELECT
      auction_id,
      auction_close,
      delivery_start,
      delivery_end,
      "PT1H" AS duration,
      corridor_code,
      from_area,
      to_area,
      cable_info,
      schedule_type,
      ftroption,
      horizon_name,
      maintenances,
      result_comment,
      offered_capacity,
      requested_capacity,
      allocated_capacity,
      product_id,
      auction_price,
      result_additional_message,
      xn_rule,
      winning_parties_eic_code,
      operational_message,
      product_comment,
      bid_party_count,
      product_offered_capacity,
      product_atc,
      product_resold_capacity,
      product_winner_party_count,
      cancelled,
      updated_at_source,
      "POWER" AS commodity,
      license,
      "jao" AS data_source,
      "jao" AS data_system,
      created_at,
      created_by,
      updated_at,
      updated_by,
      table_id
FROM {catalog_prefix}silver.{schema_prefix}market_data_and_fundamentals.jao_auction"""

spark.sql(load_query)
