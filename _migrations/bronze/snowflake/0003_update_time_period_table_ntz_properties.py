# Databricks notebook source
from pyspark.sql import SparkSession
from pyspark.sql.functions import col

spark = SparkSession.builder.getOrCreate()

# COMMAND ----------


catalog_prefix = dbutils.widgets.get("env_catalog_identifier")  # type: ignore # noqa: F821
schema_prefix = dbutils.widgets.get("schema_prefix")  # type: ignore # noqa: F821
schema = "snowflake"
table = "time_period"


# COMMAND ----------

table_name = f"{catalog_prefix}bronze.{schema_prefix}{schema}.{table}"
if spark.catalog.tableExists(table_name):
    df = spark.read.table(table_name)

    # List of columns to cast
    columns_to_cast = [field.name for field in df.schema.fields if field.dataType.simpleString() == "timestamp"]

    for column_name in columns_to_cast:
        df = df.withColumn(column_name, col(column_name).cast("timestamp_ntz"))

    # Enable the timestampNtz feature
    spark.sql(f"ALTER TABLE {table_name} SET TBLPROPERTIES ('delta.feature.timestampNtz' = 'supported')")

    (df.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(table_name))
