# Databricks notebook source
from databricks.sdk.runtime import dbutils
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, concat, current_timestamp, lit, to_timestamp

spark = SparkSession.builder.getOrCreate()

# COMMAND ----------

# Configuration variables
catalog_prefix = dbutils.widgets.get("env_catalog_identifier")  # type: ignore
schema_prefix = dbutils.widgets.get("schema_prefix")  # type: ignore

# COMMAND ----------
# Load parquet files
precip_parquet_file = "/Volumes/dna_prod_staging/volue/historical_data/precip_history.parquet"
temperature_parquet_file = "/Volumes/dna_prod_staging/volue/historical_data/temperature_history.parquet"
precip_df = spark.read.parquet(precip_parquet_file)
temperature_df = spark.read.parquet(temperature_parquet_file)

# COMMAND ----------
# Define curve names
curve_name_precip = concat(lit("rre "), col("area"), lit(" weather30 gwh cet h sa"))
curve_name_temperature = concat(lit("tt "), col("area"), lit(" con °c weather40 cet h sa"))

# COMMAND ----------
# Apply standardization to match time_series_sa and time_series_s table schemas
precip_df = (
    precip_df.withColumn("curve_name", curve_name_precip)
    .withColumn("value_at", to_timestamp(col("time")))
    .withColumn("value", col("gwh"))
    .drop("time", "area", "gwh")
)
temperature_df = (
    temperature_df.withColumn("curve_name", curve_name_temperature)
    .withColumn("value_at", to_timestamp(col("time")))
    .withColumn("value", col("celcius"))
    .drop("time", "area", "celcius")
)

# COMMAND ----------
# Define a clear created_by/updated_by name for traceability
str_created_by = "ad-hoc-migration-script-0010"
# Create audit columns
precip_df = (
    precip_df.withColumn("created_at", current_timestamp())
    .withColumn("created_by", lit(str_created_by))
    .withColumn("updated_at", current_timestamp())
    .withColumn("updated_by", lit(str_created_by))
)
temperature_df = (
    temperature_df.withColumn("created_at", current_timestamp())
    .withColumn("created_by", lit(str_created_by))
    .withColumn("updated_at", current_timestamp())
    .withColumn("updated_by", lit(str_created_by))
)

# COMMAND ----------
# Append to bronze layer
time_series_sa_table_name = f"{catalog_prefix}bronze.{schema_prefix}volue.time_series_sa"
precip_df.write.mode("append").saveAsTable(time_series_sa_table_name)
temperature_df.write.mode("append").saveAsTable(time_series_sa_table_name)

# Delete manually ingested data
spark.sql(f"""
DELETE FROM {catalog_prefix}bronze.{schema_prefix}volue.time_series_sa WHERE created_by = 'hadrien.copponnex@axpo.com'
""")
# Some data were accidentally ingested in the time_series_s. They should be deleted as well.
spark.sql(f"""
DELETE FROM {catalog_prefix}bronze.{schema_prefix}volue.time_series_s WHERE created_by = 'hadrien.copponnex@axpo.com'
""")

# After that, we should refresh entirely the tables `temperature_consumption` and `precipitation` in the silver layer.
