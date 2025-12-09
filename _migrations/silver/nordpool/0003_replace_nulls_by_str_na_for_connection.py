# Databricks notebook source
from databricks.sdk.runtime import dbutils
from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()

# COMMAND ----------
catalog_prefix = dbutils.widgets.get("env_catalog_identifier")  # type: ignore
schema_prefix = dbutils.widgets.get("schema_prefix")  # type: ignore

# COMMAND ----------

# Read tables and replace null values in the 'connection' columns with 'N/A'
table_name = f"{catalog_prefix}silver.{schema_prefix}nordpool.physical_flow_cross_border"

query = f"UPDATE {table_name} SET connection = 'N/A' WHERE connection IS NULL;"
print(query)
spark.sql(query)

query = f"ALTER TABLE {table_name} ALTER COLUMN connection SET NOT NULL;"
print(query)
spark.sql(query)

# Drop and recreate primary key constraint to reflect the changes
print("Dropping and recreating primary key constraint...")
spark.sql(f"""
    ALTER TABLE {table_name} DROP CONSTRAINT pk_{table_name.split(".")[-1]};
""")

# Define constraints for pk columns
for col in ["delivery_start", "delivery_end", "from_area", "to_area", "connection"]:
    spark.sql(f"""
        ALTER TABLE {table_name}
        ALTER COLUMN {col} SET NOT NULL
    """)

spark.sql(f"""
    ALTER TABLE {table_name} ADD CONSTRAINT pk_{table_name.split(".")[-1]}
    PRIMARY KEY (delivery_start, delivery_end, from_area, to_area, connection);
""")

print("done")
