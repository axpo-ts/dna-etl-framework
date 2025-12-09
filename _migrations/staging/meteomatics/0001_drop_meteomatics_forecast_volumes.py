# Databricks notebook source
from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()


# COMMAND ----------

catalog_prefix = dbutils.widgets.get("env_catalog_identifier")  # type: ignore # noqa: F821
schema_prefix = dbutils.widgets.get("schema_prefix")  # type: ignore # noqa: F821

# COMMAND ----------

volume_name_list = [
    "geopotential_height_100hpa_m_ecmwf_ens_p1d_ensemble",
    "geopotential_height_500hpa_m_ecmwf_ens_p1d_ensemble",
    "msl_pressure_hpa_ecmwf_ens_p1d_ensemble",
    "precip_24h_mm_ecmwf_ens_p1d_ensemble",
    "t_mean_2m_24h_c_ecmwf_ens_p1d_ensemble",
    "wind_speed_u_200hpa_ms_ecmwf_ens_p1d_ensemble",
    "wind_speed_u_20hpa_ms_ecmwf_ens_p1d_ensemble",
    "wind_speed_v_200hpa_ms_ecmwf_ens_p1d_ensemble",
    "global_rad_w_ecmwf_ens_p1d_ensemble",
]


for volume_name in volume_name_list:
    drop_sql = f"""
    DROP VOLUME IF EXISTS {catalog_prefix}staging.{schema_prefix}meteomatics.{volume_name}
    """
    print(drop_sql)
    spark.sql(drop_sql)
