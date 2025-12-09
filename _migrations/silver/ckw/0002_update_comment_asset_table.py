# Databricks notebook source

from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()

# COMMAND ----------


catalog_prefix = dbutils.widgets.get("env_catalog_identifier")  # type: ignore # noqa: F821
schema_prefix = dbutils.widgets.get("schema_prefix")  # type: ignore # noqa: F821
schema = "ckw"
table = "asset"

table_name = f"{catalog_prefix}silver.{schema_prefix}{schema}.{table}"

spark.sql(f"""ALTER TABLE {table_name}
              ALTER COLUMN plant_voltage_level_id
              COMMENT 'Identifier for the plants voltage level';""")
