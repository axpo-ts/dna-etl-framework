=====================================
Data Model Structure and Organization
=====================================

Overview
========

The data model in this repository follows a standardized architecture using abstract base classes and PySpark schema definitions. Models are defined as Python classes under ``src/data_platform/data_model/`` following a strict *catalog / schema / table* hierarchy with type safety and reusability at its core.

Core Architecture
=================

The data model is built around two main components:

**AbstractTableModel**
    An abstract base class that defines the contract for all table models. Subclasses must implement the ``identifier`` and ``schema`` properties, with all other properties being optional.

**TableIdentifier**
    A frozen dataclass that encapsulates table location information (catalog, schema, table name) and provides string representation functionality.

**StaticTableModel**
    A concrete implementation of ``AbstractTableModel`` used for defining static table structures.

Folder Structure Convention
===========================

The data model follows a strict ``{catalog_name}/{schema_name}/{table_name}.py`` convention:

.. code-block:: text

    src/data_platform/data_model/
    ├── {catalog_name}/              # Top-level catalog (e.g., bronze, silver, gold)
    │   ├── {schema_name}/           # Schema within catalog
    │   │   ├── {table_name}.py      # Individual table model
    │   │   └── __init__.py          # Schema-level imports
    │   └── __init__.py              # Catalog-level imports
    └── abstract_table_model.py     # Base classes and utilities

Example Structure
-----------------

.. code-block:: text

    src/data_platform/data_model/
    ├── abstract_table_model.py     # AbstractTableModel, TableIdentifier
    ├── bronze/
    │   ├── __init__.py
    │   └── swissgrid/
    │       ├── __init__.py
    │       ├── activation_signal.py
    │       ├── afrr_activations.py
    │       └── attributes.py
    └── silver/
        ├── __init__.py
        └── market_data_and_fundamentals/
            ├── __init__.py
            └── activation_signal_picasso_swissgrid.py

Abstract Table Model Contract
=============================

Required Properties
-------------------

All table models must implement these abstract properties:

``identifier`` : TableIdentifier
    The table's location (catalog, schema, name) wrapped in a ``TableIdentifier`` object.

``schema`` : StructType
    PySpark ``StructType`` describing all data columns with their types, nullability, and metadata.

Optional Properties
-------------------

The following properties have default implementations but can be overridden:

``comment`` : str | None
    Table description/documentation.

``sources`` : Sequence[dict[str, str]] | None
    List of source table references with catalog, schema, and table information.

``partition_cols`` : Sequence[str]
    Column names used for table partitioning.

``primary_keys`` : Sequence[str]
    Column names that form the primary key.

``liquid_cluster_cols`` : Sequence[Sequence[str]]
    Column groupings for liquid clustering optimization.

``tags`` : dict[str, str] | None
    Key-value metadata tags for governance and classification.

``license`` : Literal | None
    License identifier for the table data.

Utility Methods
---------------

``full_name`` : str
    Returns the fully qualified table name (``catalog.schema.table``).

``schema_ddl`` : str
    Returns Spark-compatible DDL string for table columns.

``writer_options()`` : dict[str, str]
    Returns options dictionary for table creation operations.

``as_dict()`` : dict
    Serializes the table definition for catalog UIs and auditing.

Standard Table Model Template
=============================

.. code-block:: python

    from pyspark.sql.types import StructType, StructField, StringType, TimestampType
    from data_platform.data_model import StaticTableModel, TableIdentifier

    # Define table model
    table_name_table = StaticTableModel(
        identifier=TableIdentifier(
            catalog="catalog_name",
            schema="schema_name",
            name="table_name"
        ),
        schema=StructType([
            StructField("column1", StringType(), False, {"comment": "Column description"}),
            StructField("column2", TimestampType(), True),
        ]),
        comment="Table description",
        sources=[
            {
                "source_catalog": "source_catalog",
                "source_schema": "source_schema",
                "source_table_name": "source_table"
            }
        ],
        primary_keys=("column1",),
        partition_cols=(),
        tags={
            "confidentiality_level": "C2_internal",
            "data_owner": "Team Name",
            "source_system": "System Name",
        }
    )

Real-World Example
------------------

Bronze Layer Table (``bronze/swissgrid/activation_signal.py``):

.. code-block:: python

    from pyspark.sql.types import FloatType, StringType, StructField, StructType, TimestampType
    from data_platform.data_model import StaticTableModel, TableIdentifier

    activation_signal_table = StaticTableModel(
        identifier=TableIdentifier("bronze", "swissgrid", "activation_signal"),
        schema=StructType([
            StructField("DatumZeit", StringType(), True),
            StructField("Stellsignal_abs_aktiv", FloatType(), True),
            StructField("_rescued_data", StringType(), True),
        ]),
        comment="Activation signals table from Swissgrid",
        sources=[
            {"source_catalog": "staging", "source_schema": "swissgrid", "source_table_name": "activation_signal"}
        ],
        primary_keys=("DatumZeit", "Stellsignal_abs_aktiv"),
    )

``Please note`` that audit columns like created_at, created_by, updated_at, and updated_by are not included in the schema definition as they are automatically managed by the Create Table task.

Silver Layer Table (``silver/market_data_and_fundamentals/activation_signal_picasso_swissgrid.py``):

.. code-block:: python

    from pyspark.sql import functions as f
    from pyspark.sql.types import FloatType, StringType, StructField, StructType, TimestampType
    from data_platform.data_model import StaticTableModel, TableIdentifier

    activation_signal_picasso_swissgrid_table = StaticTableModel(
        identifier=TableIdentifier(
            catalog="silver",
            schema="market_data_and_fundamentals",
            name="activation_signal_picasso_swissgrid"
        ),
        schema=StructType([
            StructField("time_stamp_utc", TimestampType(), False, {"comment": "Delivery start time"}),
            StructField("volume", FloatType(), True, {"comment": "Secondary energy demand"}),
            StructField("unit_type", StringType(), True, {"comment": "Unit of measurement"}),
            StructField("license", StringType(), True, {"comment": "License identifier"}),
        ]),
        comment="Holds activation signals from Picasso",
        sources=[
            {"source_catalog": "bronze", "source_schema": "swissgrid", "source_table_name": "activation_signal"},
            {"source_catalog": "bronze", "source_schema": "swissgrid", "source_table_name": "afrr_activations"},
        ],
        license=f.lit("PICASSO_LICENSE"),
        tags={
            "confidentiality level": "C2_internal",
            "data_owner": "David Perraudin",
            "

Table Creation
==============

The data model definitions are utilized by ETL tasks to create and manage Delta tables in the Databricks environment. Two primary tasks are provided:
``CreateTable``
``UpdateTableMetadata``

CreateTable
=================
   **Description:**
      The ``CreateTable`` task ensures that the specified Delta table exists in the target catalog and schema, creating it if necessary using the provided table model definition. This includes schema, partitioning, and table-level metadata.

   **Parameters:**
      - **context** (*TaskContext*): Task execution context, including Spark session and environment prefixes.
      - **table_model** (*AbstractTableModel*): Table model definition containing schema, identifier, and metadata.

   **Usage Example:**

   .. code-block:: python

      from data_platform.data_model.silver.swissgrid import activated_volume_afrr_energy_table
      from data_platform.etl.load.create_table import CreateTable

      CreateTable(
          context=context,
          table_model=activated_volume_afrr_energy_table,
      ).execute()

UpdateTableMetadata
===================

   **Description:**
      The ``UpdateTableMetadata`` task updates table-level metadata (such as comments and tags) for an existing Delta table, ensuring alignment with the latest table model definition.

   **Parameters:**
      - **context** (*TaskContext*): Task execution context, including Spark session and environment prefixes.
      - **table_model** (*AbstractTableModel*): Table model definition containing updated metadata and schema.

   **Usage Example:**

   .. code-block:: python

      from data_platform.data_model.silver.swissgrid import activated_volume_afrr_energy_table
      from data_platform.etl.load.update_table_metadata import UpdateTableMetadata

      UpdateTableMetadata(
          context=context,
          table_model=activated_volume_afrr_energy_table,
      ).execute()
