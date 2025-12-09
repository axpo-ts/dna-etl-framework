# Databricks notebook source
from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()

# COMMAND ----------


catalog_prefix = dbutils.widgets.get("env_catalog_identifier")  # type: ignore # noqa: F821
schema_prefix = dbutils.widgets.get("schema_prefix")  # type: ignore # noqa: F821

# COMMAND ----------

spark.sql(f"""
    ALTER TABLE {catalog_prefix}silver.{schema_prefix}attributes.pronovo ADD COLUMN data_source STRING FIRST;
""")

spark.sql(f"""
     UPDATE {catalog_prefix}silver.{schema_prefix}attributes.pronovo SET data_source = 'pronovo';
""")

spark.sql(f"""
    ALTER TABLE {catalog_prefix}silver.{schema_prefix}attributes.pronovo ADD COLUMN data_system STRING FIRST;
""")

spark.sql(f"""
     UPDATE {catalog_prefix}silver.{schema_prefix}attributes.pronovo SET data_system = 'geocat';
""")
