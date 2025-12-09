# Databricks notebook source

###############
# DESCRIPTION #
###############

# This script unifies the `tagged_forecast_volue` tables with the `forecast_volue` tables.
# First, it adds a new column `tag` to the `forecast_volue` tables.
# Then, it appends the `tagged_forecast_volue` tables into the `forecast_volue` tables.
# Finally, it drops the `tagged_forecast_volue` tables.


# COMMAND ----------

from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()

# Get variables
catalog_prefix = dbutils.widgets.get("env_catalog_identifier")  # type: ignore # noqa: F821
schema_prefix = dbutils.widgets.get("schema_prefix")  # type: ignore # noqa: F821

# Set script parameters
catalog = "silver"
schema = "market_data_and_fundamentals"

# List all prefixes to compose the table names
# This list contains the prefixes of the tagged and non-tagged tables that will be unified
table_prefix_list = [
    "production_wind",
    "production_spv",
    "precipitation",
    "consumption",
    "spot_price",
    "temperature_consumption",
    "reservoir_hydro",
]

# For some prefixes, there is no tagged tables: for example, there is not `reservoir_hydro_tagged_forecast_volue` table.
# Then, we need a list to ignore these prefixes in the append iteration.
inexistent_tagged_tables_list = [
    "reservoir_hydro",
]

# COMMAND ----------

# Now, we iterate over the prefixes to perform the operations on the tables
for prefix_table in table_prefix_list:
    # Compose table name to include a new column "tag"
    table_name_forecast_volue = f"{prefix_table}_forecast_volue"

    # Compose the full path to the forecast_volue table
    table_path_forecast_volue = f"{catalog_prefix}{catalog}.{schema_prefix}{schema}.{table_name_forecast_volue}"

    # List of tuples with column names, data types, and positions to add
    columns_to_add = [
        ("tag", "STRING", "AFTER issued_at"),
    ]

    print(f"Table: {table_name_forecast_volue} / Step: adding new column")

    #  Apply the ALTER TABLE statement to add the new columns
    for column_name, data_type, position in columns_to_add:
        if column_name not in spark.table(table_path_forecast_volue).columns:
            alter_table_stmt = (
                f"ALTER TABLE {table_path_forecast_volue} ADD COLUMNS ({column_name} {data_type} {position})"
            )
            spark.sql(alter_table_stmt)

    # Now, we append `tagged_forecast_volue` into the `forecast_volue` and delete the original `tagged_forecast_volue`
    # We ignore the non-existent tagged tables for this iteration
    if prefix_table not in inexistent_tagged_tables_list:
        # Prepare the path for the tagged_forecast_volue table
        table_name_tagged_forecast_volue = f"{prefix_table}_tagged_forecast_volue"
        table_path_tagged_forecast_volue = (
            f"{catalog_prefix}{catalog}.{schema_prefix}{schema}.{table_name_tagged_forecast_volue}"
        )

        # Read the tables
        df_tagged_forecast_volue = spark.read.table(table_path_tagged_forecast_volue)
        df_forecast_volue = spark.read.table(table_path_forecast_volue)

        # Guarantees that the columns from tagged tables will be in the same order of the non-tagged ones
        df_tagged_forecast_volue_validated = df_tagged_forecast_volue.select(df_forecast_volue.columns)

        print(f"Table: {table_name_tagged_forecast_volue} / Step: appending to {table_name_forecast_volue}")

        # Append the tagged table to the table_path_forecast_volue
        df_tagged_forecast_volue_validated.write.mode("append").saveAsTable(table_path_forecast_volue)

        print(f"Table: {table_name_tagged_forecast_volue} / Step: deleting table")

        # Drop the original tagged_forecast_volue table
        # Because it was unified with the forecast_volue table and we no longer need it
        spark.sql(f"DROP TABLE IF EXISTS {table_path_tagged_forecast_volue}")
