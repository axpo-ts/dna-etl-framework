# Databricks notebook source

from pyspark.sql import SparkSession

from data_platform.data_model.silver.nordpool import physical_flow_cross_border_table
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


CreateTable(context=context, table_model=physical_flow_cross_border_table).execute()

load_query = f"""INSERT INTO {catalog_prefix}silver.{schema_prefix}nordpool.physical_flow_cross_border
SELECT
      delivery_start,
      delivery_end,
      duration,
      from_area,
      to_area,
      connection,
      import,
      export,
      net_position,
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
FROM {catalog_prefix}silver.{schema_prefix}market_data_and_fundamentals.nordpool_physical_flow"""

spark.sql(load_query)
