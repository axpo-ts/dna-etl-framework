# Databricks notebook source
from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()

# COMMAND ----------


catalog_prefix = dbutils.widgets.get("env_catalog_identifier")  # type: ignore # noqa: F821
schema_prefix = dbutils.widgets.get("schema_prefix")  # type: ignore # noqa: F821

table_name_target = (
    f"{catalog_prefix}silver.{schema_prefix}market_data_and_fundamentals.precipitation_forecast_meteomatics"
)
column_name = "delivery_end"

if spark.catalog.tableExists(table_name_target):
    df = spark.table(table_name_target)
    if column_name not in df.columns:
        # rerwite the resulting table
        (
            spark.sql(f"""
            SELECT
                curve_name,
                time AS delivery_start,
                TIMESTAMPADD(DAY, 1, time) AS delivery_end,
                latitude,
                longitude,
                precip_24h,
                created_at,
                created_by,
                updated_at,
                updated_by
            FROM {table_name_target}
            """)
            .write.format("delta")
            .mode("overwrite")
            .option("overwriteSchema", "true")
            .saveAsTable(table_name_target)
        )
