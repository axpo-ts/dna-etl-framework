# Databricks notebook source
from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()

# COMMAND ----------


catalog_prefix = dbutils.widgets.get("env_catalog_identifier")  # type: ignore # noqa: F821
schema_prefix = dbutils.widgets.get("schema_prefix")  # type: ignore # noqa: F821
azure_subscription_env = dbutils.widgets.get("azure_subscription_env")  # type: ignore # noqa: F821

# COMMAND ----------
ROW_FILTER_STATEMENTS = [
    "CREATE OR REPLACE FUNCTION "
    "`{catalog_prefix}bronze`.`{schema_prefix}data_governance`.fx_row_filter_license (license STRING) "
    "RETURNS BOOLEAN RETURN ("
    "IS_ACCOUNT_GROUP_MEMBER('admins') OR "
    "IS_ACCOUNT_GROUP_MEMBER('CL-AZ-SUBS-dna-{azure_subscription_env}-central-owner') OR "
    "is_member(license))"
]

formatted_statements = [
    stmt.format(
        catalog_prefix=catalog_prefix, schema_prefix=schema_prefix, azure_subscription_env=azure_subscription_env
    )
    for stmt in ROW_FILTER_STATEMENTS
]

# Check if function exists first
check_query = f"""
SELECT COUNT(*) as cnt
FROM system.information_schema.routines
WHERE routine_catalog = '{catalog_prefix}bronze'
  AND routine_schema = '{schema_prefix}data_governance'
  AND routine_name = 'fx_row_filter_license'
"""

exists = spark.sql(check_query).collect()[0]["cnt"] > 0

if exists:
    for sql in formatted_statements:
        spark.sql(sql)
