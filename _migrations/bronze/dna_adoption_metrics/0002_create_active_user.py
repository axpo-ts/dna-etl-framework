# Databricks notebook source
from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()

# COMMAND ----------


catalog_prefix = dbutils.widgets.get("env_catalog_identifier")  # type: ignore # noqa: F821
schema_prefix = dbutils.widgets.get("schema_prefix")  # type: ignore # noqa: F821
schema = "dna_adoption_metrics"

catalog_name = f"{catalog_prefix}bronze"
schema_name = f"{schema_prefix}{schema}"
write_table_name = f"{catalog_name}.{schema_name}.active_user"

create_ddl = f"""CREATE TABLE IF NOT EXISTS {write_table_name} (
  timestamp TIMESTAMP NOT NULL,
  workspace_id STRING NOT NULL,
  cnt DOUBLE,
  list STRING,
  created_at TIMESTAMP,
  created_by STRING,
  updated_at TIMESTAMP,
  updated_by STRING,
  CONSTRAINT `pk_active_user` PRIMARY KEY (`timestamp`, `workspace_id`))
USING delta;"""

spark.sql(create_ddl)

read_tables = {
    "active_users_1week_dev": "4027174922259360",
    "active_users_1week_fcty": "4071172135320476",
    "active_users_1week_hackathon": "3391241334329287",
    "active_users_1week_prod": "2433690965938898",
    "active_users_1week_snbx": "3549828462202385",
    "active_users_1week_test": "1881335288368228",
}


for read_table, workspace_id in read_tables.items():
    read_table_name = f"{catalog_name}.{schema_name}.{read_table}"
    spark.sql(f"""
    INSERT INTO {write_table_name}
    SELECT `timestamp`
            , '{workspace_id}' AS workspace_id
            , cnt
            , list
            , `timestamp` AS created_at
            , CURRENT_USER() AS created_by
            , `timestamp` AS updated_at
            , CURRENT_USER() AS updated_by
    FROM {read_table_name};
  """)
    spark.sql(f"DROP TABLE IF EXISTS {read_table_name}")
