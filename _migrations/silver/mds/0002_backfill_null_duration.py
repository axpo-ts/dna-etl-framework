# Databricks notebook source

from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()

# COMMAND ----------

catalog_prefix = dbutils.widgets.get("env_catalog_identifier")  # type: ignore # noqa: F821
schema_prefix = dbutils.widgets.get("schema_prefix")  # type: ignore # noqa: F821

attribute_query = f"""
    SELECT mdo_id,
           max(resolution_iso) as duration 
    FROM {catalog_prefix}silver.{schema_prefix}mds.attribute
    WHERE column_name = 'DeliveryStart'
    GROUP BY mdo_id
"""

attribute_df = spark.sql(attribute_query)
attribute_df.createOrReplaceTempView("attribute")

backfill_tables = ["price_end_of_day_settlement", "curve_fx_forward", "price_forward_curve"]


def merge_duration(catalog_prefix: str, schema_prefix: str, table: str) -> None:
    """Merge duration from attribute table to target table where duration is null."""
    target_table_name = f"{catalog_prefix}silver.{schema_prefix}mds.{table}"

    merge_sql = f"""
        MERGE INTO {target_table_name} target
        USING attribute source
        ON target.mdo_id = source.mdo_id
        WHEN MATCHED AND target.duration IS NULL THEN UPDATE SET target.duration = source.duration"""

    spark.sql(merge_sql)


for table in backfill_tables:
    print(f"Merging table: {table}")
    merge_duration(catalog_prefix, schema_prefix, table)
