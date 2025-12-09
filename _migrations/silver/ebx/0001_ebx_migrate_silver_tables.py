# Databricks notebook source

from pyspark.sql import SparkSession

from data_platform.data_model.silver.ebx import (
    book_table,
    business_unit_table,
    customer_group_table,
    legal_entity_table,
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

spark.sql(f"DROP TABLE IF EXISTS {context.full_unitycatalog_name(book_table.identifier)}")
spark.sql(f"DROP TABLE IF EXISTS {context.full_unitycatalog_name(business_unit_table.identifier)}")
spark.sql(f"DROP TABLE IF EXISTS {context.full_unitycatalog_name(customer_group_table.identifier)}")
spark.sql(f"DROP TABLE IF EXISTS {context.full_unitycatalog_name(legal_entity_table.identifier)}")

CreateTable(context=context, table_model=book_table).execute()
CreateTable(context=context, table_model=business_unit_table).execute()
CreateTable(context=context, table_model=customer_group_table).execute()
CreateTable(context=context, table_model=legal_entity_table).execute()

load_query = f"""INSERT INTO {context.full_unitycatalog_name(book_table.identifier)}
SELECT
    id,
    book_owner,
    book_type,
    branch_name,
    business_unit,
    credit_risk_division_name,
    legal_entity,
    name,
    party_group,
    status,
    trading_type,
    'ebx' AS data_source,
    'ebx' AS data_system,
    created_at,
    created_by,
    updated_at,
    updated_by,
    table_id
FROM {catalog_prefix}silver.{schema_prefix}counterparty_data.book_ebx"""

spark.sql(load_query)

load_query = f"""INSERT INTO {context.full_unitycatalog_name(business_unit_table.identifier)}
SELECT
    id,
    address1,
    address2,
    city,
    country,
    description,
    eic_code,
    fax,
    legal_entity,
    long_name,
    mail_code,
    name,
    parent_id,
    party_group,
    phone,
    state,
    status,
    'ebx' AS data_source,
    'ebx' AS data_system,
    created_at,
    created_by,
    updated_at,
    updated_by,
    table_id
FROM {catalog_prefix}silver.{schema_prefix}counterparty_data.business_unit_ebx"""

spark.sql(load_query)

load_query = f"""INSERT INTO {context.full_unitycatalog_name(customer_group_table.identifier)}
SELECT
    customer_group_id,
    customer_group_name,
    'ebx' AS data_source,
    'ebx' AS data_system,
    created_at,
    created_by,
    updated_at,
    updated_by,
    table_id
FROM {catalog_prefix}silver.{schema_prefix}counterparty_data.customergroup_ebx"""

spark.sql(load_query)

load_query = f"""INSERT INTO {context.full_unitycatalog_name(legal_entity_table.identifier)}
SELECT
    legal_entity_code,
    lei_code,
    legal_entity_bu_be_code,
    acer_code,
    account_name,
    accounting_area,
    comment,
    confidentiality,
    correspondence_language,
    customer_group,
    deleted,
    intercompany_relation,
    invoicing_language,
    irrelevant_counterparty,
    legal_entity_name1,
    legal_entity_name2,
    legal_entity_name3,
    legal_entity_name4,
    legal_entity_type,
    purchasing_status,
    validity_status,
    workflow_status,
    registered_address_country_code,
    relevant_for_mdlm,
    trading_locked,
    valid_from,
    valid_to,
    'ebx' AS data_source,
    'ebx' AS data_system,
    created_at,
    created_by,
    updated_at,
    updated_by,
    table_id
FROM {catalog_prefix}silver.{schema_prefix}counterparty_data.legal_entity_ebx"""

spark.sql(load_query)
