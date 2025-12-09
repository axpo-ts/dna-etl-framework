# Databricks notebook source
from pyspark.sql import SparkSession

"""
The purpose of this migration script is to rename the Swissgrid activation signal table from
activation_signal_picasso_swissgrid to swissgrid_activation_signal_picasso.
"""


spark = SparkSession.builder.getOrCreate()
catalog_prefix = dbutils.widgets.get("env_catalog_identifier")  # type: ignore # noqa: F821
schema_prefix = dbutils.widgets.get("schema_prefix")  # type: ignore # noqa: F821
# COMMAND ----------

base_path = f"{catalog_prefix}silver.{schema_prefix}market_data_and_fundamentals"

# Table rename mapping
rename_mapping = {
    # timeseries
    "activation_signal_picasso_swissgrid": "swissgrid_activation_signal_picasso"
}

for old_table, new_table in rename_mapping.items():
    full_old = f"{base_path}.{old_table}"
    full_new = f"{base_path}.{new_table}"
    if spark.catalog.tableExists(full_old):
        spark.sql(f"ALTER TABLE {full_old} RENAME TO {full_new}")
