# Databricks notebook source

from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()


# COMMAND ----------
catalog_prefix = dbutils.widgets.get("env_catalog_identifier")  # type: ignore # noqa: F821
schema_prefix = dbutils.widgets.get("schema_prefix")  # type: ignore # noqa: F821
catalog_name = dbutils.widgets.get("catalog_name")  # type: ignore # noqa: F821

schema = "montel"
tables = ["price_volume_trade", "price_volume_trade_statistic"]
for table in tables:
    full_table_name = f"{catalog_prefix}{catalog_name}.{schema_prefix}{schema}.{table}"
    if spark.catalog.tableExists(full_table_name):
        print(f"Updating commodity column in table {full_table_name}")
        spark.sql(f"""MERGE INTO {full_table_name} AS tgt
                    USING {catalog_prefix}{catalog_name}.{schema_prefix}{schema}.attribute AS src
                    ON tgt.symbol_key = src.symbol_key
                    WHEN MATCHED AND tgt.commodity IS NULL THEN
                    UPDATE SET tgt.commodity = src.commodity_type""")
    else:
        print(f"Table {full_table_name} does not exist.")
