# Databricks notebook source
from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()

# COMMAND ----------


catalog_prefix = dbutils.widgets.get("env_catalog_identifier")  # type: ignore # noqa: F821
schema_prefix = dbutils.widgets.get("schema_prefix")  # type: ignore # noqa: F821
schema = "dna_adoption_metrics"

table_map = {
    "num_users_access_list": "user_interaction_component",
    "num_users_access_list2": "contribution_ownership_metric",
    "writeback_gold": "writeback_table_name_gold",
    "num_rows_prod": "num_rows_prod",
}

new_fields = ["created_at TIMESTAMP", "created_by STRING", "updated_at TIMESTAMP", "updated_by STRING"]

for old_table, new_table in table_map.items():
    old_table_name = f"{catalog_prefix}bronze.{schema_prefix}{schema}.{old_table}"
    table_name = f"{catalog_prefix}bronze.{schema_prefix}{schema}.{new_table}"

    # Create the new table if it does not exist
    if old_table_name != table_name:
        spark.sql(f"CREATE TABLE IF NOT EXISTS {table_name} AS SELECT * FROM {old_table_name}")
    # Change data type from varchar to string
    columns = spark.sql(f"DESCRIBE {table_name}").filter("data_type = 'varchar(65000)'").select("col_name").collect()
    for col in columns:
        spark.sql(f"ALTER TABLE {table_name} CHANGE COLUMN {col['col_name']} {col['col_name']} STRING")

    # Add new fields to the table
    for field in new_fields:
        spark.sql(f"ALTER TABLE {table_name} ADD COLUMNS ({field})")

    # Make the timestamp column non-nullable
    spark.sql(f"""
    ALTER TABLE {table_name}
    ALTER COLUMN `timestamp` SET NOT NULL
    """)

    # Add primary key constraint if it does not exist
    constraints = spark.sql(f"SHOW TBLPROPERTIES {table_name}").collect()
    constraint_exists = any(f"pk_{new_table}" in row["key"] for row in constraints)

    if not constraint_exists:
        spark.sql(f"""
        ALTER TABLE {table_name}
        ADD CONSTRAINT pk_{new_table} PRIMARY KEY (`timestamp`)
        """)

    # Update the new fields with appropriate values
    spark.sql(f"""
    UPDATE {table_name}
    SET created_at = `timestamp`,
        created_by = CURRENT_USER(),
        updated_at = `timestamp`,
        updated_by = CURRENT_USER()
    """)

    # Drop the old table if it is different from the new table
    if old_table_name != table_name:
        spark.sql(f"DROP TABLE IF EXISTS {old_table_name}")
