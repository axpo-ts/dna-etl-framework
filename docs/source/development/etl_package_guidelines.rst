=================================================
ETL Package
=================================================

Overview
========

The ETL logic and data model in this repository are designed for **modularity**, **maintainability**, and **testability**. This document outlines:

* The structure and philosophy of the ``etl`` package
* How to reuse data model table objects for robust ETL pipelines
* Local development setup and configuration
* A practical usage example
* Guidance for extending the library as requirements evolve

ETL Folder Structure
====================

The ETL codebase is organized by the three core ETL phases, plus shared utilities:

.. code-block:: text

    src/data_platform/etl/
    ├── __init__.py                 # Package initialization and public exports
    ├── extract/                    # Data-extraction tasks and helpers
    ├── transform/                  # Data-transformation logic
    ├── load/                       # Data-loading tasks
    └── core/                       # Shared utilities and helpers
        ├── config_loader.py        # Configuration loading utilities
        └── task_context.py         # Task execution context

Key Architectural Principles
----------------------------

* **Standard ETL Pattern:** Clear separation of *Extract*, *Transform*, and *Load* phases
* **Single Responsibility:** Each module addresses a specific aspect of the ETL process
* **Modular Design:** Tasks are self-contained and reusable across pipelines
* **Type Safety:** Strong typing and schema validation throughout
* **Environment Isolation:** Development and production environments are isolated through configuration

Local Development Setup
========================

To run notebooks locally with proper environment isolation, you need to configure catalog and schema prefixes.

Configuration File
------------------

Create a ``local_dev.yml`` file at the **root of the project** with the following structure:

.. code-block:: yaml

    local_dev:
      catalog_prefix: dna_dev_
      schema_prefix: dev_<username>_

Replace ``<username>`` with your actual username to ensure personal isolation during development.

Loading Configuration
---------------------

At the beginning of your notebook or pipeline, load the configuration and initialize the context:

.. code-block:: python

    from databricks.sdk.runtime import dbutils

    from data_platform.data_model.bronze.swissgrid import attributes_table
    from data_platform.etl import BatchTableUpsert, CreateTable
    from data_platform.etl.core.config_loader import ConfigLoader
    from data_platform.etl.core.task_context import TaskContext

    # Load configuration prefixes for environment isolation
    params = ConfigLoader(
        deployment_file="jao_deployment.yml", # to load the default values if not defined.
        dbutils=dbutils,
    ).all()

    # Initialize task context with prefixes
    catalog_prefix = params.get("catalog_prefix")
    schema_prefix = params.get("schema_prefix")
    context = TaskContext(catalog_prefix=catalog_prefix, schema_prefix=schema_prefix)

    CreateTable(context=context, table_model=attributes_table).execute()

How Isolation Works
-------------------

The **catalog_prefix** and **schema_prefix** are automatically applied by the ``TaskContext`` to ensure:

* **Catalog isolation:** Tables are created in ``dna_dev_bronze`` instead of ``bronze``
* **Schema isolation:** Personal schemas like ``dev_john_swissgrid`` instead of ``swissgrid``
* **No conflicts:** Multiple developers can work simultaneously without interfering with each other's data

Data Model Integration
======================

The ETL package is tightly integrated with the data model definitions. **Table model objects** are imported directly into ETL tasks and provide:

* **Schema enforcement:** Use of PySpark ``StructType`` for validation and select operations
* **Centralized metadata:** Table name, schema, audit columns, tags, and more
* **Consistency:** Eliminates duplication and reduces risk of schema drift
* **Environment-aware naming:** Automatically applies prefixes through the context

Refer to the data model documentation for details on defining and organizing table models under `docs/source/development/data_model_definition.rst.`

Typical ETL Task Workflow
=========================

A standard ETL task will:

1. **Load configuration** and initialize the ``TaskContext``
2. **Import the relevant table model** from the data model package
3. **Extract** data from a source
4. **Transform** the data, applying schema and business logic
5. **Load** the result into the target table using the model's metadata and context

Complete Usage Example
======================

Below is a complete example showing the full workflow including configuration and context setup:

.. code-block:: python

    from databricks.sdk.runtime import dbutils
    from pyspark.sql import DataFrame

    from data_platform.data_model.bronze.swissgrid.attributes import attributes_table
    from data_platform.etl import BatchTableUpsert, CreateTable, IncrementalTableReader
    from data_platform.etl.core.config_loader import ConfigLoader
    from data_platform.etl.core.task_context import TaskContext

    # Configuration and context setup
    catalog_prefix, schema_prefix = ConfigLoader(dbutils=dbutils, env="local_dev").load_widg_prefix()
    context = TaskContext(catalog_prefix=catalog_prefix, schema_prefix=schema_prefix)

    # Create table if it doesn't exist
    CreateTable(context=context, table_model=attributes_table).execute()

    # Extract phase: load table data from a source
    df_raw: DataFrame = IncrementalTableReader(
        context=context,
        table_model=activation_signal_table,
        watermark_days=watermark_days,
        watermark_filter_col="updated_at",
    ).execute()

    # Transform phase: select and cast columns according to the model schema
    df_transformed = (
        df_raw
        .select([field.name for field in attributes_table.schema.fields])
        # Additional transformation logic here
    )

    # Load phase: upsert data using the ETL framework
    BatchTableUpsert(
        context=context,
        table_model=attributes_table,
        source_df=df_transformed
    ).execute()

ETL Task Components
-------------------

* **ConfigLoader:** Loads environment-specific configuration from YAML files
* **TaskContext:** Manages catalog/schema prefixes and provides isolation
* **CreateTable:** ETL task that creates tables based on model definitions
* **BatchTableUpsert:** ETL task that performs efficient data upserts
* **Table model objects:** Provide schema, metadata, and naming conventions

How Table Models Are Used
-------------------------

* **Schema validation:** Use ``attributes_table.schema`` to enforce column structure
* **Target table name:** The context automatically applies prefixes to ``attributes_table.full_name``
* **Write options:** Use ``attributes_table.writer_options()`` for consistent table creation
* **Metadata access:** Read ``attributes_table.comment``, ``tags``, ``sources`` for documentation and governance

Extending the Library
=====================

The ETL package is designed for **incremental extension**:

* Add new extract/transform/load modules as new data sources or requirements emerge
* Extend the data model with new tables or schemas as needed
* Enhance shared utilities in ``etl/core/`` for cross-cutting concerns (e.g., logging, configuration, error handling)
* Implement additional validation, lineage, or auditing features by leveraging the metadata in table models
* Add new configuration environments beyond ``local_dev`` for different deployment scenarios
* Make sure you code around the instance of the table data asset instance to make creating pipelines data asset driven and as easy as possible.


Creating Custom ETL Tasks
=========================

You can extend the ETL functionality by creating custom tasks that inherit from ``ETLTask``. All custom tasks must implement the ``execute()`` method and have a ``context`` attribute.

Simple Example: Reading CSV Files
---------------------------------

.. code-block:: python

    from __future__ import annotations

    from dataclasses import dataclass, field
    from pyspark.sql import DataFrame

    from data_platform.etl.core import TaskContext
    from data_platform.etl.etl_task import ETLTask


    @dataclass(kw_only=True)
    class ReadCSVTask(ETLTask):
        """Simple task to read data from a CSV file and return a DataFrame.

        Parameters
        ----------
        context : TaskContext
            Spark / logger wrapper supplied by your pipeline framework.
        file_path : str
            Path to the CSV file to read.
        header : bool
            Whether the CSV file has a header row.
        delimiter : str
            Column delimiter character.
        """

        context: TaskContext
        file_path: str
        header: bool = True
        delimiter: str = ","

        task_name: str = field(init=False, default="ReadCSVTask")

        def execute(self) -> DataFrame:
            """Execute the CSV reading task and return the DataFrame."""
            self.context.logger.info(f"Reading CSV file from: {self.file_path}")

            df = (
                self.context.spark.read
                .option("header", self.header)
                .option("delimiter", self.delimiter)
                .option("inferSchema", "true")
                .csv(self.file_path)
            )

            row_count = df.count()
            self.context.logger.info(f"Successfully read {row_count} rows from CSV")

            return df


    # Usage example
    csv_task = ReadCSVTask(
        context=context,
        file_path="/path/to/data.csv",
        header=True,
        delimiter=","
    )

    df = csv_task.execute()

Task Implementation Requirements
-------------------------------

All custom ETL tasks must follow these requirements:

* **Inherit from ETLTask:** Use ``class MyTask(ETLTask):``
* **Use @dataclass:** Decorated with ``@dataclass(kw_only=True)``
* **Have context attribute:** ``context: TaskContext`` is required
* **Implement execute() method:** The main entry point for task execution
* **Include task_name:** Set as a field with ``field(init=False, default="TaskName")``

Best Practices
==============

* **Always use the TaskContext** for environment isolation and consistent naming
* **Load configuration at the beginning** of notebooks to ensure proper isolation
* **Use table model objects** to reference schemas and table names—never hardcode them
* **Validate input DataFrames** against the model schema before loading
* **Leverage audit columns** (e.g., ``created_at``, ``updated_by``) for traceability
* **Document transformations** and data lineage using the ``comment`` and ``tags`` fields in the table model


Limitations and Roadmap
=======================

* The current library covers core some functionality ETL (e.g., automated schema evolution, dynamic partitioning, or data quality checks) will be added over time.
* Additional configuration environments (staging, production) will be supported as the library evolves.
* Contributions and suggestions for new helpers, validations, or integrations are welcome.

For details on defining and organizing table models, see the data model documentation.
