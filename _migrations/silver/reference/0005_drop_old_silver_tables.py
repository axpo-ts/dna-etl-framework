# Databricks notebook source
from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()

# COMMAND ----------


catalog_prefix = dbutils.widgets.get("env_catalog_identifier")  # type: ignore # noqa: F821
schema_prefix = dbutils.widgets.get("schema_prefix")  # type: ignore # noqa: F821
schema = "reference"
tables = ["unittype", "countryext"]

for table in tables:
    table_name = f"{catalog_prefix}silver.{schema_prefix}{schema}.{table}"

    sql_query = f"DROP TABLE IF EXISTS {table_name}"
    print(sql_query)
    spark.sql(sql_query)
