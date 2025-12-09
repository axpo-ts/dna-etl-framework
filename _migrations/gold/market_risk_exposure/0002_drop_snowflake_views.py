# Databricks notebook source
from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()

# COMMAND ----------


catalog_prefix = dbutils.widgets.get("env_catalog_identifier")  # type: ignore # noqa: F821
schema_prefix = dbutils.widgets.get("schema_prefix")  # type: ignore # noqa: F821
schema = "market_risk_exposure"
tables = [
    "market_price_catalog_cmdp",
    "delta_monthly_endur",
    "volume_hourly_endur",
    "timezone_mapping_endur",
    "peak_offpeak_mapping_endur",
    "hour_index_month_index_mapping_endur",
    "market_price_super_index_cmdp",
    "market_price_cmdp",
    "delta_monthly_delta_gamma_result_endur",
]


# COMMAND ----------

for table in tables:
    table_name = f"{catalog_prefix}gold.{schema_prefix}{schema}.{table}"

    spark.sql(f"DROP TABLE IF EXISTS {table_name}")
