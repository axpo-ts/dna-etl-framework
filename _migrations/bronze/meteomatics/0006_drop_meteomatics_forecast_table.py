# Databricks notebook source
# DBTITLE 1,Table Mappings
from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()


# COMMAND ----------

# DBTITLE 1,Catalog and Schema Configuration
catalog_prefix = dbutils.widgets.get("env_catalog_identifier")  # type: ignore # noqa: F821
schema_prefix = dbutils.widgets.get("schema_prefix")  # type: ignore # noqa: F821
schema = "meteomatics"
catalog = "bronze"


# COMMAND ----------

# DBTITLE 1,- Dropping Existing Forecast Table

delete_sql = f"""
    DROP TABLE {catalog_prefix}{catalog}.{schema_prefix}{schema}.forecast_meteomatics
    """
spark.sql(delete_sql)
