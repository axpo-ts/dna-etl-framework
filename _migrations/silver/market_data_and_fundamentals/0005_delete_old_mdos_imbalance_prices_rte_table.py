# Databricks notebook source
from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()

# COMMAND ----------
# https://github.com/axpo-ts/dna-platform-project-repo/issues/254

catalog_prefix = dbutils.widgets.get("env_catalog_identifier")  # type: ignore # noqa: F821
schema_prefix = dbutils.widgets.get("schema_prefix")  # type: ignore # noqa: F821

# COMMAND ----------

table_name = f"{catalog_prefix}silver.{schema_prefix}market_data_and_fundamentals.imbalance_price_rte"
# Check if the table has any of the specified ids and delete them if they exist
df_silver = spark.sql(f"SELECT count(*) AS total FROM {table_name} WHERE id IN (232000253, 232000254)")
total_count = df_silver.collect()[0]["total"]
print("Total count: " + str(total_count))
if total_count > 0:
    sql_stmt = f"DELETE FROM {table_name} WHERE id IN (232000253, 232000254)"
    print(sql_stmt)
    spark.sql(sql_stmt)
