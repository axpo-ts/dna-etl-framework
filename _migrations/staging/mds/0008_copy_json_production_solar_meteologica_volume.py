# Databricks notebook source
from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()

# COMMAND ----------


catalog_prefix = dbutils.widgets.get("env_catalog_identifier")  # type: ignore # noqa: F821
schema_prefix = dbutils.widgets.get("schema_prefix")  # type: ignore # noqa: F821

# COMMAND ----------

location = f"/Volumes/{catalog_prefix}staging/{schema_prefix}mds"
schema_name = f"{catalog_prefix}staging.{schema_prefix}mds"
volume_name_old = "production_solar_meteologica"
volume_name_new = "meteolog_curves_power_production"
available_volumes = [x[0] for x in spark.sql(f"SHOW VOLUMES IN {schema_name}").select("volume_name").collect()]
if volume_name_old in available_volumes:
    volume_contents = spark.sql(f"LIST '{location}/{volume_name_old}'").select("name").collect()
    json_files = [x[0] for x in volume_contents if ".json" in x[0]]
    for filename in json_files:
        dbutils.fs.mv(f"{location}/{volume_name_old}/{filename}", f"{location}/{volume_name_new}/{filename}")  # type: ignore # noqa: F821
