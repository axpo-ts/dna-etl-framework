# Databricks notebook source
from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()

# COMMAND ----------


catalog_prefix = dbutils.widgets.get("env_catalog_identifier")  # type: ignore # noqa: F821
schema_prefix = dbutils.widgets.get("schema_prefix")  # type: ignore # noqa: F821

table_name_target = f"{catalog_prefix}silver.{schema_prefix}market_data_and_fundamentals.temperature_river_volue"
table_name_attributes = f"{catalog_prefix}silver.{schema_prefix}attributes.volue"
column_name = "delivery_end"

if spark.catalog.tableExists(table_name_target):
    df = spark.table(table_name_target)
    if column_name not in df.columns:
        # rerwite the resulting table
        (
            spark.sql(f"""
            SELECT
                data.curve_name,
                data.value_at AS delivery_start,
                CASE
                    WHEN attributes.duration = 'PT15M' THEN data.value_at + INTERVAL 15 MINUTE
                    WHEN attributes.duration = 'PT1H' THEN data.value_at + INTERVAL 1 HOUR
                    ELSE data.value_at
                END AS delivery_end,
                data.value,
                data.created_at,
                data.created_by,
                data.updated_at,
                data.updated_by
            FROM {table_name_target} data
            LEFT JOIN {table_name_attributes} attributes
                ON data.curve_name = attributes.name
            """)
            .write.format("delta")
            .mode("overwrite")
            .option("overwriteSchema", "true")
            .saveAsTable(table_name_target)
        )
