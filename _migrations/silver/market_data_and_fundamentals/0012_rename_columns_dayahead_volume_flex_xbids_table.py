# Databricks notebook source
from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()

# COMMAND ----------


catalog_prefix = dbutils.widgets.get("env_catalog_identifier")  # type: ignore # noqa: F821
schema_prefix = dbutils.widgets.get("schema_prefix")  # type: ignore # noqa: F821

table_name = f"{catalog_prefix}silver.{schema_prefix}market_data_and_fundamentals.dayahead_volume_flex_xbids"
if spark.catalog.tableExists(table_name):
    df = spark.table(table_name)
    if "timestamp_utc_von" in df.columns:
        (
            spark.sql(f"""
            SELECT
                timestamp_utc_von AS delivery_start,
                delivery_end,
                timeseries_id_ch_actual_too AS value_column,
                value_ch_actual_too AS value, -- noqa: RF04
                datetime_update_utc AS updated_at_source,
                created_at AS created_at,
                created_by AS created_by,
                updated_at AS updated_at,
                updated_by AS updated_by
            FROM {table_name}
            """)
            .write.format("delta")
            .mode("overwrite")
            .option("overwriteSchema", "true")
            .saveAsTable(table_name)
        )
