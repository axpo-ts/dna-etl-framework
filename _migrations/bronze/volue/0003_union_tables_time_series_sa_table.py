# Databricks notebook source
from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()

# COMMAND ----------

catalog_prefix = dbutils.widgets.get("env_catalog_identifier")  # type: ignore # noqa: F821
schema_prefix = dbutils.widgets.get("schema_prefix")  # type: ignore # noqa: F821

# COMMAND ----------
input_table_1 = f"{catalog_prefix}bronze.{schema_prefix}volue.res_hydro_wtr_h_sa"
input_table_2 = f"{catalog_prefix}bronze.{schema_prefix}volue.pro_hydro_ror_h_sa"
input_table_3 = f"{catalog_prefix}bronze.{schema_prefix}volue.pro_res_hydro_h_sa"
input_table_4 = f"{catalog_prefix}bronze.{schema_prefix}volue.res_hydro_sgw_h_sa"
output_table = f"{catalog_prefix}bronze.{schema_prefix}volue.time_series_sa"
if (
    spark.catalog.tableExists(input_table_1)
    and spark.catalog.tableExists(input_table_2)
    and spark.catalog.tableExists(input_table_3)
    and spark.catalog.tableExists(input_table_4)
):
    (
        spark.read.table(input_table_1)
        .unionByName(spark.read.table(input_table_2))
        .unionByName(spark.read.table(input_table_3))
        .unionByName(spark.read.table(input_table_4))
        .drop("curve_type")
        .write.format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .saveAsTable(output_table)
    )
