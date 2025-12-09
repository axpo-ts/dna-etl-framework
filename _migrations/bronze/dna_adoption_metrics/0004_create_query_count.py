# Databricks notebook source
from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()

# COMMAND ----------


catalog_prefix = dbutils.widgets.get("env_catalog_identifier")  # type: ignore # noqa: F821
schema_prefix = dbutils.widgets.get("schema_prefix")  # type: ignore # noqa: F821
schema = "dna_adoption_metrics"

catalog_name = f"{catalog_prefix}bronze"
schema_name = f"{schema_prefix}{schema}"
write_table_name = f"{catalog_name}.{schema_name}.query_count"

create_ddl = f"""CREATE TABLE IF NOT EXISTS {write_table_name} (
  timestamp TIMESTAMP NOT NULL,
  workspace_id STRING NOT NULL,
  sql_editor DOUBLE,
  dataiku DOUBLE,
  powerbi DOUBLE,
  dbx_notebook DOUBLE,
  dbx_jobs DOUBLE,
  dbx_sql_dashboard DOUBLE,
  dbx_catalog_explorer DOUBLE,
  dbx_sql_genie DOUBLE,
  r_odbc DOUBLE,
  created_at TIMESTAMP,
  created_by STRING,
  updated_at TIMESTAMP,
  updated_by STRING,
  CONSTRAINT `pk_query_count` PRIMARY KEY (`timestamp`, `workspace_id`))
USING delta;"""

spark.sql(create_ddl)

read_tables = {
    "query_count_1week_fcty": "4071172135320476",
    "query_count_1week_prod": "2433690965938898",
    "query_count_1week_snbx": "3549828462202385",
}


for read_table, workspace_id in read_tables.items():
    read_table_name = f"{catalog_name}.{schema_name}.{read_table}"
    spark.sql(f"""
    INSERT INTO {write_table_name}
    SELECT `timestamp`
            , '{workspace_id}' AS workspace_id
            , sql_editor
            , dataiku
            , powerbi
            , dbx_notebook
            , dbx_jobs
            , dbx_sql_dashboard
            , dbx_catalog_explorer
            , dbx_sql_genie
            , r_odbc
            , `timestamp` AS created_at
            , CURRENT_USER() AS created_by
            , `timestamp` AS updated_at
            , CURRENT_USER() AS updated_by
    FROM {read_table_name};
  """)
    spark.sql(f"DROP TABLE IF EXISTS {read_table_name}")
