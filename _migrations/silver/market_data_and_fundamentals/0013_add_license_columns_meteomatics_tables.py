# Databricks notebook source
from pyspark.sql import SparkSession
from pyspark.sql.functions import lit

spark = SparkSession.builder.getOrCreate()

# COMMAND ----------

# Configuration variables
catalog_prefix = dbutils.widgets.get("env_catalog_identifier")  # type: ignore # noqa: F821
schema_prefix = dbutils.widgets.get("schema_prefix")  # type: ignore # noqa: F821

target_catalog_name = "silver"
target_schema_name = "market_data_and_fundamentals"

columns_to_add = "license"
column_values = "METEOMAT_WEATHER_API_METEOMAT"
tables = [
    "wind_speed_meteomatics",
    "temperature_meteomatics",
    "precipitation_forecast_meteomatics",
    "geopotential_height_meteomatics",
    "mean_sea_level_pressure_meteomatics",
]

# Process each table
for table in tables:
    full_table_name = f"{catalog_prefix}silver.{schema_prefix}{target_schema_name}.{table}"

    if spark.catalog.tableExists(full_table_name):
        # Read the table into a DataFrame
        df = spark.table(full_table_name)

        # Check if the column to add already exists
        if columns_to_add not in df.columns:
            # Add the new column and reorder the columns to place it before 'created_at'
            if "created_at" in df.columns:
                # Get the index of the 'created_at' column
                created_at_index = df.columns.index("created_at")
                # Reorder columns
                new_columns_order = df.columns[:created_at_index] + [columns_to_add] + df.columns[created_at_index:]
            else:
                # Add new column at the end if 'created_at' is not present
                new_columns_order = [*df.columns, columns_to_add]

            # Add the new column with the specified value
            df = df.withColumn(columns_to_add, lit(column_values))

            # Reorder the DataFrame columns
            df = df.select(*new_columns_order)

        # Overwrite the table with the updated DataFrame
        df.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(full_table_name)
