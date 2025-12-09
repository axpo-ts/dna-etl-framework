[Home](../README.md)

# Schema migrations

## Overview

A schema migration framework to update schema/tables in the platform without the need to manually delete/modify a table in different environments.

### Naming convention for migration scripts:

```
XXXX_{operation}_{object_name}_{object_type}.py
```

- `XXXX` - numbering of migration script to keep in order of when they need to be applied.
- `operation` - type of activity being applied in the migration.
- `object_name` - name of the table/schema being modified.
- `object_type` - type of object ['table', 'schema', 'volume']

### How To:

Configure each migration script to have the following starting blocks, in order to enable spark session and retrieval of the schema and catalog prefixes.

```python
# Databricks notebook source
from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()

# COMMAND ----------

catalog_prefix = dbutils.widgets.get("env_catalog_identifier")  # type: ignore # noqa: F821
schema_prefix = dbutils.widgets.get("schema_prefix")  # type: ignore # noqa: F821

# COMMAND ----------

```

#### Steps:

**Rules to adding a migration script:**

- each step/operation in a migration must be split into its own migration script
  - this is to prevent confusion in what the migrations are doing and to prevent running a further step in a migration if a step beforehand fails
- you must follow the naming convention
- you must validate the migration script in your own schema with the existing schema, to validate the migation works as expected

**Testing migration scripts:**

- run the orchestration workflow of the schemas you need to update if you dont have the latest schemas in your prefix in development environment
- add your migration script under the folder `_migration` / `catalog_name` / `object_schema` / `your_migration_script.py`
- write the migration operation such that it runs even if that table/schema/volume does not exist anymore. This is needed when running the migration job next time in the DEV environment.
- update the `resources` / `general` / `schema_migrations` / `schema_migrations_deployment_variables.yaml` file - adding your `object_schema` i.e. mds, if it doesnt existing
- run the migration workflow
- validate migration worked as expected
- make any changes to the orchestration workflow to reflect the change in operation (if needed). i.e. update column name/table name in configs for the workflow you are updating
- run the orchestration workflow again to validate the changes dont break the existing workflow
- create PR

**Limitations:**

- We're limited to the operations on Delta Lake, so we need to follow convensions on updates to schemas provided by them in docs `https://docs.delta.io/latest/delta-batch.html#-ddlschema`.
- Views on tables - the core challenge when you operate with views is resolving the schemas. If you alter a Delta table schema, you must recreate derivative views to account for any additions to the schema. For instance, if you add a new column to a Delta table, you must make sure that this column is available in the appropriate views built on top of that base table.

#### Operations:

**Drop tables:**
Only parameters that need to change in each environment will be the catalog prefix and the schema prefix.

```python
spark.sql(f"DROP TABLE IF EXISTS {catalog_prefix}bronze.{schema_prefix}volue.volue_curves_attributes")
```

**Rename tables:**
Only parameters that need to change in each environment will be the catalog prefix and the schema prefix. Make sure that the table exists before renaming it. If the same table name appears more than once, define it as a variable such that you know for sure that the same table name is used each time.

```python
table_name_old = "the old table name"
table_name_new = "the new table name"
if spark.catalog.tableExists(table_name_old):
    spark.sql(f"ALTER TABLE {table_name_old} RENAME TO {table_name_new}")
```

**Rename volumes:**
Same as for table, except that the code used to check if a volume exists is different:

```python
schema_name = f"{catalog_prefix}staging.{schema_prefix}mds"
volume_name_old = "the old volume name"
volume_name_new = "the new volume name"
available_volumes = [x[0] for x in spark.sql(f"SHOW VOLUMES IN {schema_name}").select("volume_name").collect()]
if volume_name_old in available_volumes:
    spark.sql(f"ALTER VOLUME {schema_name}.{volume_name_old} RENAME TO {schema_name}.{volume_name_new}")
```

**Change data types of columns:**
Only parameters that need to change in each environment will be the catalog prefix and the schema prefix. Make sure that the table exists before applying the operation.

```python
table_name = f"table name"
column_name = "change_data_type_column"
if spark.catalog.tableExists(table_name):
(
    spark.read.table(table_name)
    .withColumn(column_name, col(column_name).cast("int"))
    .write.format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(table_name)
)
```

**Add columns:**
Make sure that the table exists and that the column has not already been added to the table.

```python
table_name = "the table name"
column_name = "the column name"
if spark.catalog.tableExists(table_name):
    columns = spark.table(table_name).columns
    if column_name not in columns:
        spark.sql(f"ALTER TABLE {table_name} ADD COLUMNS ({column_name} data_type [COMMENT col_comment] [FIRST|AFTER colA_name], ...)")
```

**Replace columns:**
Make sure that the table exists.

```python
table_name = "the table name"
if spark.catalog.tableExists(table_name):
    spark.sql(f"ALTER TABLE {table_name} REPLACE COLUMNS (col_name1 col_type1 [COMMENT col_comment1], ...)")
```

**Rename columns:**
Make sure that the table exists and that the column has not already been renamed.

```python
table_name = "the table name"
column_name_old = "old column name"
column_name_new = "old column name"
if spark.catalog.tableExists(table_name):
    columns = spark.table(table_name).columns
    if column_name_old in columns:
        spark.sql(f"ALTER TABLE {table_name} RENAME COLUMN {column_name_old} TO {column_name_new}")
```

**Drop columns:**
Make sure that the table and the column exists.

```python
table_name = "the table name"
column_name = "the column name"
if spark.catalog.tableExists(table_name):
    columns = spark.table(table_name).columns
    if column_name in columns:
        spark.sql(f"ALTER TABLE {table_name} DROP COLUMN {column_name}")
```

**Move files between volumes**
To move all `json` files from an old volume to a new one. We need to check that the old volume exists and then return all files in the old volume that match the search criteria.

```python
location = f"/Volumes/{catalog_prefix}staging/{schema_prefix}mds"
schema_name = f"{catalog_prefix}staging.{schema_prefix}mds"
volume_name_old = "the old volume name"
volume_name_new = "the new volume name"
available_volumes = [x[0] for x in spark.sql(f"SHOW VOLUMES IN {schema_name}").select("volume_name").collect()]
if volume_name_old in available_volumes:
    volume_contents = spark.sql(f"LIST '{location}/{volume_name_old}'").select("name").collect()
    json_files = [x[0] for x in volume_contents if '.json' in x[0]]
    for filename in json_files:
        dbutils.fs.mv(f'{location}/{volume_name_old}/{filename}', f'{location}/{volume_name_new}/{filename}')  # type: ignore # noqa: F821
```

[Home](../README.md)
