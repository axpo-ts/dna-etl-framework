# Databricks notebook source
from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()

# COMMAND ----------
# https://github.com/axpo-ts/dna-platform-etl/issues/914

catalog_prefix = dbutils.widgets.get("env_catalog_identifier")  # type: ignore # noqa: F821
schema_prefix = dbutils.widgets.get("schema_prefix")  # type: ignore # noqa: F821

# COMMAND ----------

table_name = f"{catalog_prefix}bronze.{schema_prefix}mds.mdo_attributes"
ids_to_delete = "536013751, 536145251, 536145501, 536145751, 536324251, 536324501"
# Check if the table has any of the specified ids and delete them if they exist
df_bronze = spark.sql(f"SELECT count(*) AS total FROM {table_name} WHERE mdoid IN ({ids_to_delete})")
total_count = df_bronze.collect()[0]["total"]
print("Total count: " + str(total_count))
if total_count > 0:
    sql_stmt = f"DELETE FROM {table_name} WHERE mdoid IN ({ids_to_delete})"
    spark.sql(sql_stmt)
