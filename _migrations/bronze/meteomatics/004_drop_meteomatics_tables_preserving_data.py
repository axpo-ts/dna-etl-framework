# Databricks notebook source
# DBTITLE 1,Table Mappings
from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()

pv_table_names = {
    "global_rad_w_mm_euro1k_pt1h": "value",
    "snow_depth_cm_mm_euro1k_pt1h": "value",
    "clear_sky_rad_w_mm_euro1k_pt1h": "value",
}
non_pv_table_names = {
    "t_mean_2m_24h_c_ecmwf_era5_p1d": "t_mean_2m_24h",
    "precip_24h_mm_mix_p1d": "precip_24h",
    "geopotential_height_500hpa_m_ecmwf_era5_p1d": "geopotential_height_500hPa",
    "msl_pressure_hpa_ecmwf_era5_p1d": "msl_pressure",
    "wind_speed_u_10hpa_ms_ecmwf_era5_p1d": "wind_speed_u_10hPa",
    "wind_speed_u_200hpa_ms_ecmwf_era5_p1d": "wind_speed_u_200hPa",
    "wind_speed_v_10hpa_ms_ecmwf_era5_p1d": "wind_speed_v_10hPa",
    "wind_speed_v_200hpa_ms_ecmwf_era5_p1d": "wind_speed_v_200hPa",
    "wind_speed_10hpa_ms_ecmwf_era5_p1d": "wind_speed_10hPa",
    "wind_speed_200hpa_ms_ecmwf_era5_p1d": "wind_speed_200hPa",
    "t_mean_850hpa_24h_c_ecmwf_era5_p1d": "t_mean_850hPa_24h",
}

# COMMAND ----------

# DBTITLE 1,Catalog and Schema Configuration
catalog_prefix = dbutils.widgets.get("env_catalog_identifier")  # type: ignore # noqa: F821
schema_prefix = dbutils.widgets.get("schema_prefix")  # type: ignore # noqa: F821
schema = "meteomatics"
catalog = "bronze"

# COMMAND ----------

# DBTITLE 1,Creating Meteomatics Table with Delta Properties
spark.sql(f"""
CREATE SCHEMA IF NOT EXISTS {catalog_prefix}{catalog}.{schema_prefix}{schema}
""")

spark.sql(f"""
CREATE TABLE {catalog_prefix}{catalog}.{schema_prefix}{schema}.actual_meteomatics (
  curve_name STRING,
  time TIMESTAMP,
  value DOUBLE,
  latitude DECIMAL(7,4),
  longitude DECIMAL(7,4),
  created_at TIMESTAMP,
  created_by STRING,
  updated_at TIMESTAMP,
  updated_by STRING)
USING delta
CLUSTER BY (curve_name,latitude,longitude,time)
TBLPROPERTIES (
  'delta.checkpointPolicy' = 'v2',
  'delta.enableDeletionVectors' = 'true',
  'delta.enableRowTracking' = 'true',
  'delta.feature.allowColumnDefaults' = 'supported',
  'delta.feature.deletionVectors' = 'supported',
  'delta.feature.rowTracking' = 'supported',
  'delta.feature.v2Checkpoint' = 'supported')
""")

# COMMAND ----------

# DBTITLE 1,Union PV Tables and Insert into Actual Meteomatics
# Read and union all pv_tables
union_df = None
for table in pv_table_names:
    select_sql = f"""
    SELECT
    curve_name,
    time,
    latitude,
    longitude,
    value,
    created_at,
    created_by,
    updated_at,
    updated_by
    FROM {catalog_prefix}{catalog}.{schema_prefix}{schema}.{table}
    """
    df = spark.sql(select_sql)

    if union_df is None:
        union_df = df
    else:
        union_df = union_df.unionByName(df)

# Insert the data into the actual_meteomatics table
union_df.createOrReplaceTempView("union_pv_tables")

insert_sql = f"""

INSERT INTO {catalog_prefix}{catalog}.{schema_prefix}{schema}.actual_meteomatics
SELECT 
    curve_name,
    time,
    value,
    latitude,
    longitude,
    created_at,
    created_by,
    updated_at,
    updated_by
FROM union_pv_tables
ORDER BY updated_at
"""
spark.sql(insert_sql)

# COMMAND ----------

non_pv_table_names

# COMMAND ----------

# Read and union all pv_tables
union_df = None
for key, value in non_pv_table_names.items():
    select_sql = f"""
    SELECT
    '{key}' AS curve_name,
    time,
    latitude,
    longitude,
    {value} AS value,
    created_at,
    created_by,
    updated_at,
    updated_by
    FROM {catalog_prefix}{catalog}.{schema_prefix}{schema}.{key}
    """
    print(select_sql)
    df = spark.sql(select_sql)

    if union_df is None:
        union_df = df
    else:
        union_df = union_df.unionByName(df)

# Insert the data into the actual_meteomatics table
union_df.createOrReplaceTempView("union_non_pv_tables")

insert_sql = f"""

INSERT INTO {catalog_prefix}{catalog}.{schema_prefix}{schema}.actual_meteomatics
SELECT 
    curve_name,
    time,
    value,
    latitude,
    longitude,
    created_at,
    created_by,
    updated_at,
    updated_by
FROM union_non_pv_tables
ORDER BY updated_at
"""
spark.sql(insert_sql)

# COMMAND ----------

# DBTITLE 1,Distinct Curve Names from Meteomatics Table
df = spark.sql(f"""SELECT curve_name FROM {catalog_prefix}{catalog}.{schema_prefix}{schema}.actual_meteomatics""")
distinct_df = df.distinct()
display(distinct_df.select("curve_name"))

# COMMAND ----------

# DBTITLE 1,Aggregating All Table Names
all_tables = [key for key in non_pv_table_names] + [key for key in pv_table_names]
all_tables

# COMMAND ----------

# DBTITLE 1,- Dropping Existing Tables in Schema
for table in all_tables:
    drop_sql = f"""
    DROP TABLE IF EXISTS {catalog_prefix}{catalog}.{schema_prefix}{schema}.{table}
    """
    spark.sql(drop_sql)
