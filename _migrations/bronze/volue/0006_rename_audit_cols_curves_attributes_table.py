# Databricks notebook source
from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()

# COMMAND ----------


catalog_prefix = dbutils.widgets.get("env_catalog_identifier")  # type: ignore # noqa: F821
schema_prefix = dbutils.widgets.get("schema_prefix")  # type: ignore # noqa: F821

# COMMAND ----------

table_name = f"{catalog_prefix}bronze.{schema_prefix}volue.curves_attributes"
if spark.catalog.tableExists(table_name):
    df = spark.table(table_name)
    if "CreatedAt" in df.columns:
        (
            df.withColumnsRenamed(
                {
                    "CreatedAt": "created_at",
                    "CreatedBy": "created_by",
                    "UpdatedAt": "updated_at",
                    "UpdatedBy": "updated_by",
                }
            )
            .write.format("delta")
            .mode("overwrite")
            .option("overwriteSchema", "true")
            .saveAsTable(table_name)
        )
