# Databricks notebook source
from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()

# COMMAND ----------


catalog_prefix = dbutils.widgets.get("env_catalog_identifier")  # type: ignore # noqa: F821
schema_prefix = dbutils.widgets.get("schema_prefix")  # type: ignore # noqa: F821

# COMMAND ----------

table_name = f"{catalog_prefix}bronze.{schema_prefix}data_governance.data_quality_metrics"
if spark.catalog.tableExists(table_name):
    df = spark.table(table_name)
    for col in ["passing_records", "failing_records", "total_number_of_records"]:
        if col in df.columns and dict(df.dtypes)[col] == "int":
            df = df.withColumn(col, df[col].cast("bigint"))
    df.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(table_name)
