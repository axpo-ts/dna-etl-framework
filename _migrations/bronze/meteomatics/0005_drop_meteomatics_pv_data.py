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

# DBTITLE 1,- Dropping Existing PV data as duration is changing from PT1H TO PT15M

delete_sql = f"""
    DELETE FROM {catalog_prefix}{catalog}.{schema_prefix}{schema}.actual_meteomatics
    WHERE curve_name IN(
    'global_rad_w_mm_euro1k_pt1h',
    'snow_depth_cm_mm_euro1k_pt1h',
    'clear_sky_rad_w_mm_euro1k_pt1h'
    )
    """
spark.sql(delete_sql)
