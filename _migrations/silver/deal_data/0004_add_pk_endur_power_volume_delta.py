# Databricks notebook source
from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()

# COMMAND ----------
# Get variables
catalog_prefix = dbutils.widgets.get("env_catalog_identifier")  # type: ignore # noqa: F821
schema_prefix = dbutils.widgets.get("schema_prefix")  # type: ignore # noqa: F821

# Set script parameters
catalog = "silver"
schema = "deal_data"
table_name = "endur_power_volume_delta"
primary_keys = "timestamp, period_id, deal_number, transaction_status_id, parameter_sequence_number, schedule_id, volume_type_id, internal_portfolio_id, internal_business_unit_id, external_portfolio_id, external_business_unit_id, settlement_type_id, projection_index_id, location_id"  # noqa: E501
# COMMAND ---------- Compose target table string
target_table = f"{catalog_prefix}{catalog}.{schema_prefix}{schema}.{table_name}"

# COMMAND ---------- Split primary keys intro array
pk = primary_keys.replace(" ", "").split(",")

# COMMAND ---------- Check table and columns
check_table = spark.catalog.tableExists(target_table)

if check_table:
    df = spark.table(target_table)
    for column in pk:
        if column not in df.columns:
            check_table = False

if not check_table:
    dbutils.notebook.exit("Table or columns do not exist")  # type: ignore # noqa: F821

# COMMAND ---------- Generate and run SET NOT NULL commands for Primary Keys Columns
pk_set_columns_not_null = map(lambda col: "ALTER TABLE " + target_table + " ALTER COLUMN " + col + " SET NOT NULL", pk)
for cmd in pk_set_columns_not_null:
    spark.sql(cmd)

# COMMAND ---------- Remove PK Constraint from target table
spark.sql(f"ALTER TABLE {target_table} DROP CONSTRAINT IF EXISTS pk_{table_name}")

# COMMAND ---------- Add PK Constraint to target table
spark.sql(f"ALTER TABLE {target_table} ADD CONSTRAINT pk_{table_name} PRIMARY KEY ({primary_keys})")
