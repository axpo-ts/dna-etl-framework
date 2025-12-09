# Databricks notebook source
from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()

# COMMAND ----------


catalog_prefix = dbutils.widgets.get("env_catalog_identifier")  # type: ignore # noqa: F821
schema_prefix = dbutils.widgets.get("schema_prefix")  # type: ignore # noqa: F821

# COMMAND ----------

table_name = f"{catalog_prefix}bronze.{schema_prefix}meteomatics.t_mean_2m_24h_c_ecmwf_era5_p1d"
if spark.catalog.tableExists(table_name):
    df = spark.table(table_name)
    spark.sql(f"ALTER TABLE {table_name} SET TBLPROPERTIES ('delta.columnMapping.mode' = 'name')")  # noqa: E501
    if "CreatedAt" in df.columns:
        spark.sql(f"ALTER TABLE {table_name} RENAME COLUMN CreatedAt TO created_at")  # noqa: E501
    if "CreatedBy" in df.columns:
        spark.sql(f"ALTER TABLE {table_name} RENAME COLUMN CreatedBy TO created_by")  # noqa: E501
    if "UpdatedAt" in df.columns:
        spark.sql(f"ALTER TABLE {table_name} RENAME COLUMN UpdatedAt TO updated_at")  # noqa: E501
    if "UpdatedBy" in df.columns:
        spark.sql(f"ALTER TABLE {table_name} RENAME COLUMN UpdatedBy TO updated_by")  # noqa: E501
