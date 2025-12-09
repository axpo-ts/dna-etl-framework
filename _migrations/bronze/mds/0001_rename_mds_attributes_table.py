# Databricks notebook source
from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()

# COMMAND ----------


catalog_prefix = dbutils.widgets.get("env_catalog_identifier")  # type: ignore # noqa: F821
schema_prefix = dbutils.widgets.get("schema_prefix")  # type: ignore # noqa: F821


input_table = f"{catalog_prefix}bronze.{schema_prefix}mds.attributes"
if spark.catalog.tableExists(input_table):
    (
        spark.read.table(input_table)
        .write.format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .saveAsTable(f"{catalog_prefix}bronze.{schema_prefix}mds.mdo_attributes")
    )
