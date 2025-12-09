# Databricks notebook source
from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()

# COMMAND ----------


catalog_prefix = dbutils.widgets.get("env_catalog_identifier")  # type: ignore # noqa: F821
schema_prefix = dbutils.widgets.get("schema_prefix")  # type: ignore # noqa: F821

# COMMAND ----------
table_name = f"{catalog_prefix}bronze.{schema_prefix}data_governance.data_dictionary"
if spark.catalog.tableExists(table_name):
    df = spark.sql(f"""
        SELECT table_type
        FROM system_tables_sharing.system_tables_information_schema.tables
        WHERE table_catalog = '{catalog_prefix}bronze'
        AND table_schema = '{schema_prefix}data_governance'
        AND table_name = 'data_dictionary'
    """)
    table_type = df.collect()[0][0]
    if table_type == "MANAGED":
        sql_stmt = f"DROP TABLE IF EXISTS {table_name}"
        print(sql_stmt)
        spark.sql(sql_stmt)
    else:
        print(f"Table {table_name} is not a MANAGED table")
