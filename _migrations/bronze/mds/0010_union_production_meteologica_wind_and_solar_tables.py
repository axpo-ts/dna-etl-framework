# Databricks notebook source
from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()

# COMMAND ----------


catalog_prefix = dbutils.widgets.get("env_catalog_identifier")  # type: ignore # noqa: F821
schema_prefix = dbutils.widgets.get("schema_prefix")  # type: ignore # noqa: F821

# COMMAND ----------
input_table_1 = f"{catalog_prefix}bronze.{schema_prefix}mds.production_wind_meteologica"
input_table_2 = f"{catalog_prefix}bronze.{schema_prefix}mds.production_solar_meteologica"
output_table = f"{catalog_prefix}bronze.{schema_prefix}mds.meteolog_curves_power_production"
if spark.catalog.tableExists(input_table_1) and spark.catalog.tableExists(input_table_2):
    (
        spark.read.table(input_table_1)
        .unionByName(spark.read.table(input_table_2))
        .write.format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .saveAsTable(output_table)
    )
