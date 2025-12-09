# Databricks notebook source
from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()

# COMMAND ----------
catalog_prefix = dbutils.widgets.get("env_catalog_identifier")  # type: ignore # noqa: F821
schema_prefix = dbutils.widgets.get("schema_prefix")  # type: ignore # noqa: F821

# COMMAND ----------

# Read tables and replace LPI name in the column "licence"
tables_names = (
    f"{catalog_prefix}silver.{schema_prefix}ckw.asset",
    f"{catalog_prefix}silver.{schema_prefix}ckw.contract",
    f"{catalog_prefix}silver.{schema_prefix}ckw.attribute",
)
for table_name in tables_names:
    print(f"Modifying table `{table_name}`")
    spark.sql(f"""
        UPDATE {table_name}
        SET license = 'DNA_CKW'
        WHERE license = 'TKM'
    """)
    print(f"The table `{table_name}` was correctly updated")
