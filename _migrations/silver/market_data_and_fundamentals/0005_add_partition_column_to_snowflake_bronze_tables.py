# Databricks notebook source
from pyspark.sql import SparkSession
from pyspark.sql.functions import to_date

spark = SparkSession.builder.getOrCreate()

# COMMAND ----------


catalog_prefix = dbutils.widgets.get("env_catalog_identifier")  # type: ignore # noqa: F821
schema_prefix = dbutils.widgets.get("schema_prefix")  # type: ignore # noqa: F821

tables = [
    ("PRICE_FORWARD_CURVES_PRICES_LATEST", "PUBLICATION_DATE"),
    ("ENDUR_EGL_DWH_DELTA_GAMMA_RESULT", "GENERATION_TIME"),
    ("ENDUR_TRAN_GPT_DELTA_BY_LEG", "GENERATION_TIME"),
    ("PRICE_FORWARD_CURVES_LATEST", "CMDP_TIMESTAMP"),
    ("VOLUME_WITH_DELTA", "REPORTING_DATE"),
    ("PRICE_FORWARD_CURVES_SUPER_INDEX_PRICES_LATEST", "TIMESTAMP"),
]

for table, partion_expr in tables:
    if spark.catalog.tableExists(f"{catalog_prefix}bronze.{schema_prefix}snowflake.{table}"):
        columns = spark.catalog.listColumns(f"{catalog_prefix}bronze.{schema_prefix}snowflake.{table}")
        partition_columns = [col.name for col in columns if col.isPartition]
        if not "__partition_by" in partition_columns:
            df = spark.read.table(f"{catalog_prefix}bronze.{schema_prefix}snowflake.{table}")
            df = df.withColumn("__partition_by", to_date(partion_expr))
            df.write.format("delta").option("overwriteSchema", "true").mode("overwrite").partitionBy(
                "__partition_by"
            ).saveAsTable(f"{catalog_prefix}bronze.{schema_prefix}snowflake.{table}")
