# Databricks notebook source

from pyspark.sql import SparkSession

from data_platform.data_model.silver.mds.price_bid_ask import (
    price_bid_ask_table,
)
from data_platform.etl.core.task_context import TaskContext
from data_platform.etl.load.create_table import CreateTable

spark = SparkSession.builder.getOrCreate()


# COMMAND ----------


# create price_bid_ask_table
catalog_prefix = dbutils.widgets.get("env_catalog_identifier")  # type: ignore # noqa: F821
schema_prefix = dbutils.widgets.get("schema_prefix")  # type: ignore # noqa: F821
schema = "mds"

context = TaskContext(
    dbutils=dbutils,  # noqa: F821
    catalog_prefix=catalog_prefix,
    schema_prefix=schema_prefix,
)

CreateTable(context=context, table_model=price_bid_ask_table).execute()

# COMMAND ----------

# Migrate MDO IDs from price_forward_curve to new tables
# MDO IDs to move to price_dayahead
mdo_ids_to_dayahead = [
    344007752,
    344140751,
    344006751,
    344008751,
    344007251,
]

# MDO IDs to move to price_end_of_day_settlement
mdo_ids_to_eod_settlement = [
    344139501,
    344139751,
    344140001,
    344141001,
    344017751,
    344015501,
    344020001,
    344004501,
    344016251,
    344014001,
    344018501,
    344003001,
]

price_forward_curve_table = f"{catalog_prefix}silver.{schema_prefix}{schema}.price_forward_curve"
price_dayahead_table = f"{catalog_prefix}silver.{schema_prefix}{schema}.price_dayahead"
price_eod_settlement_table = f"{catalog_prefix}silver.{schema_prefix}{schema}.price_end_of_day_settlement"


def get_common_columns(source_table: str, target_table: str) -> list:
    """Get list of column names that exist in both source and target tables."""
    source_cols = [col.name for col in spark.catalog.listColumns(source_table)]
    target_cols = [col.name for col in spark.catalog.listColumns(target_table)]
    # Exclude table_id and volume columns, and only include columns that exist in both
    common_cols = [col for col in source_cols if col in target_cols and col not in ["table_id", "volume"]]
    return common_cols


def migrate_mdo_ids(source_table: str, target_table: str, mdo_ids: list, table_name: str) -> None:
    """Migrate MDO IDs from source table to target table, then delete from source."""
    if not mdo_ids:
        return

    if not spark.catalog.tableExists(source_table):
        print(f"Source table {source_table} does not exist. Skipping migration.")
        return

    if not spark.catalog.tableExists(target_table):
        print(f"Target table {target_table} does not exist. Skipping migration.")
        return

    # Check if there are any rows to migrate
    count_df = spark.sql(f"SELECT COUNT(*) as cnt FROM {source_table} WHERE mdo_id IN ({','.join(map(str, mdo_ids))})")
    first_row = count_df.first()
    if first_row is None:
        print(f"Could not determine row count from {source_table}. Skipping migration.")
        return
    row_count = first_row["cnt"]

    if row_count == 0:
        print(f"No rows found in {source_table} for MDO IDs {mdo_ids}. Skipping migration.")
        return

    print(f"Migrating {row_count} rows from {source_table} to {target_table} for MDO IDs: {mdo_ids}")

    # Get common columns between source and target tables
    common_cols = get_common_columns(source_table, target_table)

    if not common_cols:
        print(f"No common columns found between {source_table} and {target_table}. Skipping migration.")
        return

    # Select columns from source table
    cols_str = ", ".join(common_cols)
    mdo_ids_str = ", ".join(map(str, mdo_ids))

    # Insert into target table
    insert_query = f"""
        INSERT INTO {target_table} ({cols_str})
        SELECT {cols_str}
        FROM {source_table}
        WHERE mdo_id IN ({mdo_ids_str})
    """

    print(f"Executing insert query for {table_name}:")
    print(insert_query)
    spark.sql(insert_query)

    # Verify insertion was successful by checking row counts
    inserted_count_df = spark.sql(f"SELECT COUNT(*) as cnt FROM {target_table} WHERE mdo_id IN ({mdo_ids_str})")
    inserted_first_row = inserted_count_df.first()
    if inserted_first_row is None:
        print(f"Could not verify insertion count from {target_table}. Skipping deletion.")
        return
    inserted_count = inserted_first_row["cnt"]

    if inserted_count >= row_count:
        print(f"Successfully inserted {inserted_count} rows into {target_table}")

        # Delete from source table
        delete_query = f"""
            DELETE FROM {source_table}
            WHERE mdo_id IN ({mdo_ids_str})
        """
        print(f"Deleting rows from {source_table}...")
        spark.sql(delete_query)

        # Verify deletion
        remaining_count_df = spark.sql(f"SELECT COUNT(*) as cnt FROM {source_table} WHERE mdo_id IN ({mdo_ids_str})")
        remaining_first_row = remaining_count_df.first()
        remaining_count = remaining_first_row["cnt"] if remaining_first_row is not None else 0

        if remaining_count == 0:
            print(f"Successfully deleted {row_count} rows from {source_table}")
        else:
            print(f"Warning: {remaining_count} rows still remain in {source_table} after deletion")
    else:
        print(f"Warning: Expected {row_count} rows but only {inserted_count} rows were inserted into {target_table}")
        print("Skipping deletion from source table due to mismatch.")


# Migrate MDO IDs to price_dayahead
if mdo_ids_to_dayahead:
    migrate_mdo_ids(price_forward_curve_table, price_dayahead_table, mdo_ids_to_dayahead, "price_dayahead")

# Migrate MDO IDs to price_end_of_day_settlement
if mdo_ids_to_eod_settlement:
    migrate_mdo_ids(
        price_forward_curve_table, price_eod_settlement_table, mdo_ids_to_eod_settlement, "price_end_of_day_settlement"
    )
