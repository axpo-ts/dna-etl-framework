# Databricks notebook source
from pyspark.sql import SparkSession
from pyspark.sql import functions as f

spark = SparkSession.builder.getOrCreate()

# COMMAND ----------
catalog_prefix = dbutils.widgets.get("env_catalog_identifier")  # type: ignore # noqa: F821
schema_prefix = dbutils.widgets.get("schema_prefix")  # type: ignore # noqa: F821

# COMMAND ----------

# Read tables and replace LPI name in the column "licence"
tables_names = (
    f"{catalog_prefix}silver.{schema_prefix}market_data_and_fundamentals.dayahead_volume_flex_epex",
    f"{catalog_prefix}silver.{schema_prefix}market_data_and_fundamentals.intraday_price_volume_flex_xbids",
)
for table_name in tables_names:
    print(f"Modifying table `{table_name}`")
    df = spark.read.table(table_name)
    df = df.withColumn(
        "license", f.when(f.col("license") == "DNA-ALL-ACCESS", "DNA_ORCA_FR").otherwise(f.col("license"))
    )
    df.write.mode("overwrite").saveAsTable(table_name)
    print(f"The table `{table_name}` was correctly modified")
