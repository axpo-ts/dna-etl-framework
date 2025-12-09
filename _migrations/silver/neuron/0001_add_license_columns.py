# Databricks notebook source
from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()

FALCON_LICENSE = "FALCON_CONSUMER-PORTF_EGL-ESP"
SALESFORCE_LICENSE = "SALESFOR_CLIENT-DATA_EGL-ESP"

# COMMAND ----------
# Get variables
catalog_prefix = dbutils.widgets.get("env_catalog_identifier")  # type: ignore # noqa: F821
schema_prefix = dbutils.widgets.get("schema_prefix")  # type: ignore # noqa: F821

# Set script parameters
catalog = "silver"
schema = "neuron"

neuron_tables = [
    "falcon_aggregations",
    "falcon_best_measure",
    "falcon_measure",
    "falcon_product_contract",
    "falcon_contract",
    "salesforce_rep_asset",
    "salesforce_rep_contract",
]

for table_name in neuron_tables:
    # Compose target table string
    target_table = f"{catalog_prefix}{catalog}.{schema_prefix}{schema}.{table_name}"

    print(f"Adding license column to table: {target_table}")

    if "license" not in spark.read.table(target_table).columns:
        # Check table and columns
        spark.sql(f"""
            ALTER TABLE {target_table}
            ADD COLUMNS (license STRING COMMENT 'License column.' AFTER src_created_at)
        """)
    else:
        print(f"Table {target_table} already has 'license' column. Skipping addition.")

    if table_name.startswith("falcon"):
        spark.sql(f"""
            UPDATE {target_table}
            SET license = '{FALCON_LICENSE}'
            WHERE license IS NULL
        """)
        print(f"Updated license column for table: {target_table}. License set to {FALCON_LICENSE}.")
    elif table_name.startswith("salesforce"):
        spark.sql(f"""
            UPDATE {target_table}
            SET license = '{SALESFORCE_LICENSE}'
            WHERE license IS NULL
        """)
        print(f"Updated license column for table: {target_table}. License set to {SALESFORCE_LICENSE}.")
    else:
        pass

print("License columns added successfully.")
