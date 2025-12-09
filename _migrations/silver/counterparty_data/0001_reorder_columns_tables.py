# Databricks notebook source
from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()

# COMMAND ----------


catalog_prefix = dbutils.widgets.get("env_catalog_identifier")  # type: ignore # noqa: F821
schema_prefix = dbutils.widgets.get("schema_prefix")  # type: ignore # noqa: F821
schema = "counterparty_data"


tables = {
    "ckw_asset": [
        "plant_number",
        "plant_creation_date",
        "device_installation_date",
        "device_sealing_date",
        "eea_commissioning_date",
        "eea_kev_from_date",
        "eea_kev_until_date",
        "device_authorized_power",
        "device_authorized_power_for_feed_in",
        "tpl_maximum_consumption_power",
        "tpl_maximum_feed_in_power",
        "eea_apparent_power",
        "plant_deleted_flag",
        "plant_voltage_level_id",
        "tariff_type_id",
        "consumption_point_deleted_flag",
        "consumption_point_location_addition_1",
        "connection_object_street",
        "connection_object_house_number_1",
        "connection_object_house_number_2",
        "connection_object_house_number_3",
        "connection_object_postal_code_1",
        "connection_object_city_town_1",
        "connection_object_city_town_2",
        "connection_object_country_id",
        "connection_object_region_id",
        "connection_object_regional_structure_id",
        "connection_object_regional_structure_name",
        "metering_point_type_id",
        "load_profile_measured_flag",
        "meter_active_power_flag",
        "eea_type_id",
        "eea_type_name",
        "eea_commissioning",
        "eea_kev_flag",
        "year_of_construction",
        "metering_point_id",
        "created_at",
        "created_by",
        "updated_at",
        "updated_by",
        "table_id",
    ]
}

for table_name, columns in tables.items():
    # Get existing columns for the table
    table_path = f"{catalog_prefix}silver.{schema_prefix}{schema}.{table_name}"
    existing_columns = set(col.name for col in spark.table(table_path).schema)

    # Check if all required columns exist
    missing_columns = [col for col in columns if col not in existing_columns]
    if missing_columns:
        raise ValueError(f"Table {table_path} is missing columns: {missing_columns}")

    # Proceed with column reordering
    for i, column in enumerate(columns):
        position_clause = "FIRST" if i == 0 else f"AFTER {columns[i - 1]}"
        sql_query = f"ALTER TABLE {table_path} ALTER COLUMN {column} {position_clause}"
        print(sql_query)
        spark.sql(sql_query)
