# Databricks notebook source

from pyspark.sql import SparkSession

from data_platform.data_model.silver.meteomatics import (
    attribute_table,
    meteomatics_cloud_cover_forecast_table,
    meteomatics_geopotential_height_table,
    meteomatics_humidity_forecast_table,
    meteomatics_precipitation_forecast_table,
    meteomatics_precipitation_table,
    meteomatics_pressure_table,
    meteomatics_production_forecast_table,
    meteomatics_snow_depth_forecast_table,
    meteomatics_soil_moisture_forecast_table,
    meteomatics_solar_radiation_forecast_table,
    meteomatics_temperature_forecast_table,
    meteomatics_temperature_table,
    meteomatics_wind_direction_forecast_table,
    meteomatics_wind_speed_forecast_table,
    meteomatics_wind_speed_table,
)
from data_platform.etl.core.task_context import TaskContext
from data_platform.etl.load.create_table import CreateTable

spark = SparkSession.builder.getOrCreate()

# COMMAND ----------


catalog_prefix = dbutils.widgets.get("env_catalog_identifier")  # type: ignore # noqa: F821
schema_prefix = dbutils.widgets.get("schema_prefix")  # type: ignore # noqa: F821

context = TaskContext(
    dbutils=dbutils,  # noqa: F821
    catalog_prefix=catalog_prefix,
    schema_prefix=schema_prefix,
)


CreateTable(context=context, table_model=attribute_table).execute()

# COMMAND ----------

CreateTable(context=context, table_model=meteomatics_geopotential_height_table).execute()


# COMMAND ----------

CreateTable(context=context, table_model=meteomatics_precipitation_forecast_table).execute()

# COMMAND ----------

CreateTable(context=context, table_model=meteomatics_precipitation_table).execute()

# COMMAND ----------

CreateTable(context=context, table_model=meteomatics_temperature_table).execute()

# COMMAND ----------

CreateTable(context=context, table_model=meteomatics_temperature_forecast_table).execute()

# COMMAND ----------

CreateTable(context=context, table_model=meteomatics_wind_speed_forecast_table).execute()

# COMMAND ----------

CreateTable(context=context, table_model=meteomatics_wind_speed_table).execute()


# COMMAND ----------

CreateTable(context=context, table_model=meteomatics_pressure_table).execute()

# COMMAND ----------

CreateTable(context=context, table_model=meteomatics_production_forecast_table).execute()

# COMMAND ----------

CreateTable(context=context, table_model=meteomatics_snow_depth_forecast_table).execute()

# COMMAND ----------

CreateTable(context=context, table_model=meteomatics_solar_radiation_forecast_table).execute()

# COMMAND ----------

CreateTable(context=context, table_model=meteomatics_wind_direction_forecast_table).execute()

# COMMAND ----------

CreateTable(context=context, table_model=meteomatics_soil_moisture_forecast_table).execute()

# COMMAND ----------

CreateTable(context=context, table_model=meteomatics_humidity_forecast_table).execute()

# COMMAND ----------

CreateTable(context=context, table_model=meteomatics_cloud_cover_forecast_table).execute()
