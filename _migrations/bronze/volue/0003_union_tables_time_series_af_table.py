# Databricks notebook source
from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()

# COMMAND ----------

catalog_prefix = dbutils.widgets.get("env_catalog_identifier")  # type: ignore # noqa: F821
schema_prefix = dbutils.widgets.get("schema_prefix")  # type: ignore # noqa: F821

# COMMAND ----------
input_table_1 = f"{catalog_prefix}bronze.{schema_prefix}volue.res_hydro_bal_h_af"
input_table_2 = f"{catalog_prefix}bronze.{schema_prefix}volue.tt_riv_min15_af"
output_table = f"{catalog_prefix}bronze.{schema_prefix}volue.time_series_af"
if spark.catalog.tableExists(input_table_1) and spark.catalog.tableExists(input_table_2):
    (
        spark.read.table(input_table_1)
        .unionByName(spark.read.table(input_table_2))
        .drop("curve_type")
        .write.format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .saveAsTable(output_table)
    )
