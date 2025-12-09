# Databricks notebook source
from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()

# COMMAND ----------


catalog_prefix = dbutils.widgets.get("env_catalog_identifier")  # type: ignore # noqa: F821
schema_prefix = dbutils.widgets.get("schema_prefix")  # type: ignore # noqa: F821

# COMMAND ----------

schema_name = f"{catalog_prefix}staging.{schema_prefix}mds"
volume_name_old = "Spot_day_ahead".lower()
volume_name_new = "epex_pos_curves_auction_clearing"
available_volumes = [x[0] for x in spark.sql(f"SHOW VOLUMES IN {schema_name}").select("volume_name").collect()]
if volume_name_old in available_volumes:
    spark.sql(f"ALTER VOLUME {schema_name}.{volume_name_old} RENAME TO {schema_name}.{volume_name_new}")
