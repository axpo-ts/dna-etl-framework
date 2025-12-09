# Databricks notebook source
from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()

# COMMAND ----------
# Get variables
catalog_prefix = dbutils.widgets.get("env_catalog_identifier")  # type: ignore # noqa: F821
schema_prefix = dbutils.widgets.get("schema_prefix")  # type: ignore # noqa: F821

# Set script parameters
catalog = "silver"
schema = "attributes"
table_name = "mds"

# COMMAND ---------- Compose target table string
target_table = f"{catalog_prefix}{catalog}.{schema_prefix}{schema}.{table_name}"

# COMMAND ----------
columns_to_add = [
    ("context", "STRING", "AFTER unit"),
    ("contract_type", "STRING", "AFTER context"),
    ("contract_period", "STRING", "AFTER contract_type"),
]


# COMMAND ---------- Check table and columns
if spark.catalog.tableExists(target_table):
    columns = spark.table(target_table).columns
    for column_name, data_type, position in columns_to_add:
        if column_name not in columns:
            # If the column does not exist, add it
            alter_table_stmt = f"ALTER TABLE {target_table} ADD COLUMNS ({column_name} {data_type} {position})"
            spark.sql(alter_table_stmt)
