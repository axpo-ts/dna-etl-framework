# Databricks notebook source

from pyspark.sql import SparkSession

from data_platform.data_model.silver.nordpool import price_dayahead_table
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


CreateTable(context=context, table_model=price_dayahead_table).execute()

load_query = f"""INSERT INTO {catalog_prefix}silver.{schema_prefix}nordpool.price_dayahead
SELECT
      delivery_start,
      delivery_end,
      duration,
      delivery_area,
      price,
      price_currency,
      price_unit,
      buy_volume,
      sell_volume,
      volume_unit,
      updated_at_source,
      "POWER" AS commodity,
      license,
      "nordpool" AS data_source,
      "nordpool" AS data_system,
      created_at,
      created_by,
      updated_at,
      updated_by,
      table_id
FROM {catalog_prefix}silver.{schema_prefix}market_data_and_fundamentals.nordpool_spot_price"""

spark.sql(load_query)
