# Databricks notebook source
from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()

# COMMAND ----------


catalog_prefix = dbutils.widgets.get("env_catalog_identifier")  # type: ignore # noqa: F821
schema_prefix = dbutils.widgets.get("schema_prefix")  # type: ignore # noqa: F821

if spark.catalog.tableExists(
    f"{catalog_prefix}silver.{schema_prefix}market_data_and_fundamentals.market_prices_catalog_cmdp"
):
    (
        spark.read.table(
            f"{catalog_prefix}silver.{schema_prefix}market_data_and_fundamentals.market_prices_catalog_cmdp"
        )
        .write.format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .saveAsTable(f"{catalog_prefix}silver.{schema_prefix}market_data_and_fundamentals.market_price_catalog_cmdp")
    )
