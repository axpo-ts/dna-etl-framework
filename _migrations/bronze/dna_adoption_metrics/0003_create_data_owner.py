# Databricks notebook source
from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()

# COMMAND ----------


catalog_prefix = dbutils.widgets.get("env_catalog_identifier")  # type: ignore # noqa: F821
schema_prefix = dbutils.widgets.get("schema_prefix")  # type: ignore # noqa: F821
schema = "dna_adoption_metrics"

catalog_name = f"{catalog_prefix}bronze"
schema_name = f"{schema_prefix}{schema}"
write_table_name = f"{catalog_name}.{schema_name}.data_owner"

create_ddl = f"""CREATE TABLE IF NOT EXISTS {write_table_name} (
  timestamp TIMESTAMP NOT NULL,
  catalog_name STRING NOT NULL,
  cnt DOUBLE,
  list STRING,
  created_at TIMESTAMP,
  created_by STRING,
  updated_at TIMESTAMP,
  updated_by STRING,
  CONSTRAINT `pk_data_owner` PRIMARY KEY (`timestamp`, `catalog_name`))
USING delta;"""

spark.sql(create_ddl)

read_tables = {"data_owners_gold": "dna_prod_gold", "data_owners_silver": "dna_prod_silver"}


for read_table, table_catalog_name in read_tables.items():
    read_table_name = f"{catalog_name}.{schema_name}.{read_table}"
    spark.sql(f"""
    INSERT INTO {write_table_name}
    SELECT `timestamp`
            , '{table_catalog_name}' AS `catalog_name`
            , cnt
            , list
            , `timestamp` AS created_at
            , CURRENT_USER() AS created_by
            , `timestamp` AS updated_at
            , CURRENT_USER() AS updated_by
    FROM {read_table_name};
  """)
    spark.sql(f"DROP TABLE IF EXISTS {read_table_name}")
