# Databricks notebook source
from datetime import datetime, timedelta

from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()


# COMMAND ----------

catalog_prefix = dbutils.widgets.get("env_catalog_identifier")  # type: ignore # noqa: F821
schema_prefix = dbutils.widgets.get("schema_prefix")  # type: ignore # noqa: F821

# COMMAND ----------

volume_name_list = [
    "clear_sky_rad_w_mm_euro1k_pt15m",
    "global_rad_w_mm_euro1k_pt15m",
    "snow_depth_cm_mm_euro1k_pt15m",
]

cutoff_time = datetime.now() - timedelta(days=3)

for volume_name in volume_name_list:
    volume_path = f"/Volumes/{catalog_prefix}staging/{schema_prefix}meteomatics/{volume_name}"
    print(f"\nChecking volume: {volume_path}")

    try:
        files = dbutils.fs.ls(volume_path)  # noqa: F821 # ignore
    except Exception as e:
        print(f"Could not list files in {volume_path}: {e}")
        continue

    # 1. Remove "checkpoint" folders in the root
    for f in files:
        if f.isDir() and f.name.strip("/").lower() == "checkpoint":
            print(f"Removing checkpoint folder: {f.path}")
            dbutils.fs.rm(f.path, recurse=True)  # noqa: F821 # ignore

    # 2. Check for old files
    for f in files:
        if not f.isDir() and f.modificationTime < cutoff_time.timestamp() * 1000:
            mod_time_str = datetime.fromtimestamp(f.modificationTime / 1000)
            print(f"Old file: {f.path} (modified {mod_time_str})")
            dbutils.fs.rm(f.path)  # noqa: F821 # ignore
