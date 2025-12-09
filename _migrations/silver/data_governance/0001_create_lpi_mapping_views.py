# Databricks notebook source

from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()


# COMMAND ----------
catalog_prefix = dbutils.widgets.get("env_catalog_identifier")  # type: ignore # noqa: F821
schema_prefix = dbutils.widgets.get("schema_prefix")  # type: ignore # noqa: F821


silver_table_map_query = f"""
CREATE OR REPLACE VIEW {catalog_prefix}silver.{schema_prefix}data_governance.view_lpi_table_map AS
SELECT * FROM {catalog_prefix}bronze.{schema_prefix}data_governance.lpi_table_map
WHERE source = 'license column'"""

spark.sql(silver_table_map_query)

silver_user_lpi_query = f"""
CREATE OR REPLACE VIEW {catalog_prefix}silver.{schema_prefix}data_governance.view_lpi_user_mapping AS
SELECT DISTINCT lum.mail as user_email,
       lum.lpi,
       concat(atr.given_name, ' ', atr.sur_name) as user_name,
       atr.given_name AS first_name,
       atr.sur_name AS last_name
FROM {catalog_prefix}silver.{schema_prefix}mdlm.lpi_user_mapping lum
JOIN {catalog_prefix}silver.{schema_prefix}mdlm.lpi_user_attributes atr
  ON lum.mail = atr.mail"""

spark.sql(silver_user_lpi_query)
