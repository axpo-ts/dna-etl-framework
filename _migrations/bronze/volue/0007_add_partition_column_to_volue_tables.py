# Databricks notebook source
from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()

# COMMAND ----------


catalog_prefix = dbutils.widgets.get("env_catalog_identifier")  # type: ignore # noqa: F821
schema_prefix = dbutils.widgets.get("schema_prefix")  # type: ignore # noqa: F821

tables = [
    ("time_series_n", "curve_name"),
    ("time_series_sa", "curve_name"),
    ("time_series_a", "curve_name"),
    ("time_series_af", "curve_name"),
]

for table, partion_expr in tables:
    if spark.catalog.tableExists(f"{catalog_prefix}bronze.{schema_prefix}volue.{table}"):
        columns = spark.catalog.listColumns(f"{catalog_prefix}bronze.{schema_prefix}volue.{table}")
        partition_columns = [col.name for col in columns if col.isPartition]
        if "curve_name" not in partition_columns:
            df = spark.read.table(f"{catalog_prefix}bronze.{schema_prefix}volue.{table}")
            df.write.format("delta").option("overwriteSchema", "true").mode("overwrite").partitionBy(
                "curve_name"
            ).saveAsTable(f"{catalog_prefix}bronze.{schema_prefix}volue.{table}")
