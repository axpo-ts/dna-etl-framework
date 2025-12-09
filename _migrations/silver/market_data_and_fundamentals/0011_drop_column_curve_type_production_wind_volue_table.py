# Databricks notebook source
from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()

# COMMAND ----------

catalog_prefix = dbutils.widgets.get("env_catalog_identifier")  # type: ignore # noqa: F821
schema_prefix = dbutils.widgets.get("schema_prefix")  # type: ignore # noqa: F821

# COMMAND ----------
target_table = f"{catalog_prefix}silver.{schema_prefix}market_data_and_fundamentals.production_wind_volue"
if spark.catalog.tableExists(target_table):
    (
        spark.read.table(target_table)
        .drop("curve_type")
        .write.format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .saveAsTable(target_table)
    )
