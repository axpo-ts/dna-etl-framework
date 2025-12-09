=================================================
ETL Package and Data Model Refactor Documentation
=================================================

Overview
========

The ETL logic and data model in this repository have been refactored to improve modularity, maintainability,
and testability.
This document:

* explains the new structure in the ``etl`` folder,
* describes the new data-model approach, and
* provides a comprehensive migration guide for moving tasks from the legacy Task Runner Notebook
  to the new package-based system.

New ETL Folder Structure
========================

The ``src/data_platform/etl/`` directory now follows a simplified, standard ETL layout organized
by the three core ETL phases plus shared utilities:

.. code-block:: text

    src/data_platform/etl/
    ├── __init__.py                 # Package initialization and public exports
    ├── extract/                    # Data-extraction tasks and helpers
    ├── transform/                  # Data-transformation logic
    ├── load/                       # Data-loading tasks
    └── core/                      # Shared utilities and helpers

Key Architectural Principles
----------------------------

* **Standard ETL Pattern** – clear separation of *Extract*, *Transform*, *Load* phases
* **Single Responsibility** – each module focuses on one aspect of the ETL process
* **Modular Design** – tasks are self-contained and reusable across pipelines
* **Type Safety** – strong typing and validation throughout the codebase

ETL Phase Organization
----------------------

* **Extract** (``etl/extract/``) – data ingestion from APIs, files, DBs, streams
* **Transform** (``etl/transform/``) – SQL / PySpark transformations, cleansing, validation
* **Load** (``etl/load/``) – writing to target systems (tables, files, warehouses, streams)
* **Core** (``etl/core/``) – configuration management, common helpers

Migration Guide: From YAML Templates to Python Notebooks
========================================================

Overview of Migration Process
-----------------------------

Migration involves moving from *YAML-based* task configuration (run by the Task Runner Notebook)
to *Python notebooks* that import and use the new ``etl`` package modules directly.

Migration Components
--------------------

* **YAML templates → Python notebooks** – replace declarative config with direct code
* **Legacy tasks (``src/dataplatform/tasks/``) → New ETL modules (``src/dataplatform/etl/``)**
* **Context-driven data flow → Explicit function arguments / returns**
* **Complex task orchestration → Simple, linear notebook execution**

Old vs New Approach: Side-by-Side
---------------------------------

Old approach (YAML + Task Runner):

.. code-block:: yaml

    # resources/source/example/templates/pipeline_template.yaml
    tasks:
      1:
        task_name: "SimpleBatchReaderTask"
        tablename: "sourcetable"
        schema_name: "bronze"
        df_key: "data"
        df_namespace: "pipeline"
      2:
        task_name: "ApplyPysparkTask"
        dfinputkey: "data"
        dfinputnamespace: "pipeline"
        transformations:
          - {"func": "addauditcols", "args": {}}
      3:
        task_name: "SimpleBatchWriterTask"
        tablename: "targettable"
        schema_name: "silver"
        df_key: "data"
        df_namespace: "pipeline"

New approach (Python + ETL):

.. code-block:: python

    # Databricks notebook: notebooks/example_pipeline.py
    from dataplatform.etl.extract.simpletable_reader import SimpleTableReaderTask
    from dataplatform.etl.transform import addauditcols
    from dataplatform.etl.load.simpletable_writer import SimpleTableWriterTask
    from data_platform.tasks.core import TaskContext

    context = TaskContext()

    # Step 1 – Extract
    source_df = SimpleTableReaderTask(
        context=context,
        format_type="delta",
        tablename="sourcetable",
        schema_name="bronze",
        catalog_name="dev",
    ).execute()

    # Step 2 – Transform
    transformed_df = addauditcols(source_df)

    # Step 3 – Load
    SimpleTableWriterTask(
        context=context,
        tablename="targettable",
        schema_name="silver",
        catalog_name="dev",
        write_mode="overwrite",
    ).execute(df=transformed_df)

Key Differences
~~~~~~~~~~~~~~~

.. list-table::
   :header-rows: 1

   * - Aspect
     - Old (YAML + Tasks)
     - New (Python + ETL)
   * - Configuration
     - External YAML files
     - Direct Python arguments
   * - Data flow
     - Context properties (``df_key``, ``df_namespace``)
     - Explicit parameters / returns
   * - Execution
     - Task Runner orchestration
     - Direct notebook execution
   * - Debugging
     - Inspect context dictionaries
     - Inspect variables & set breakpoints
   * - Testing
     - Mainly end-to-end integration
     - Unit + integration tests
   * - Reusability
     - Template-based
     - Module-based

Handling Complex Transformations
--------------------------------

Option A – **Direct PySpark** (recommended for simple logic):

.. code-block:: python

    from pyspark.sql import functions as F

    transformed_df = customers_df.withColumn(
        "email_domain", F.regexp_extract(F.col("email"), "@(.+)", 1)
    ).withColumn(
        "full_name", F.concat(F.col("firstname"), F.lit(" "), F.col("lastname"))
    )

Option B – **Custom ETL module** (recommended for reusable logic):

.. code-block:: python

    # src/dataplatform/etl/transform/customer_transformer.py
    from dataclasses import dataclass
    from pyspark.sql import DataFrame, functions as F
    from dataplatform.etl.etltask import ETLTask
    from data_platform.tasks.core import TaskContext

    @dataclass
    class CustomerDataTransformer(ETLTask):
        context: TaskContext
        task_name: str = "CustomerDataTransformer"

        def execute(self, df: DataFrame) -> DataFrame:
            return (
                df.withColumn(
                    "email_domain", F.regexp_extract(F.col("email"), "@(.+)", 1)
                ).withColumn(
                    "full_name", F.concat(F.col("firstname"), F.lit(" "), F.col("lastname"))
                )
            )

Task Mapping: Legacy → New ETL Structure
----------------------------------------

Extract tasks
~~~~~~~~~~~~~

.. list-table::
   :header-rows: 1

   * - Legacy Task
     - New ETL Module
     - Purpose
   * - ``SimpleBatchReaderTask``
     - ``etl.extract.simpletable_reader.SimpleTableReaderTask``
     - Read from tables
   * - ``SimpleBatchFileReaderTask``
     - ``etl.extract.file_reader.FileReaderTask``
     - Read from files
   * - ``SimpleSQLTableReaderTask``
     - ``etl.extract.sql_reader.SqlReaderTask``
     - Read via SQL
   * - ``ApiReaderTask``
     - ``etl.extract.api_reader.ApiReaderTask``
     - Read from APIs

Transform tasks
~~~~~~~~~~~~~~~

.. list-table::
   :header-rows: 1

   * - Legacy Task
     - New ETL Module
     - Purpose
   * - ``ApplyPysparkTask``
     - ``etl.transform.pyspark_transforms.PySparkTransformTask``
     - PySpark transformations
   * - ``ApplySQLTask``
     - ``etl.transform.sql_transforms.SqlTransformTask``
     - SQL transformations
   * - ``JoinDataFrameTask``
     - ``etl.transform.dataframe_operations.JoinTask``
     - DataFrame joins
   * - ``UnionByNameDataFrameTask``
     - ``etl.transform.dataframe_operations.UnionTask``
     - DataFrame unions

Load tasks
~~~~~~~~~~

.. list-table::
   :header-rows: 1

   * - Legacy Task
     - New ETL Module
     - Purpose
   * - ``SimpleBatchWriterTask``
     - ``etl.load.simpletable_writer.SimpleTableWriterTask``
     - Write to tables
   * - ``SimpleBatchUpsertChangesTask``
     - ``etl.load.upsert_writer.UpsertWriterTask``
     - Upsert operations
   * - ``SelectiveOverwriteTask``
     - ``etl.load.selective_writer.SelectiveOverwriteTask``
     - Selective overwrites

Key Migration Benefits
======================

Before (YAML templates):

❌ Complex environment-variable management
❌ Difficult debugging of transformation logic
❌ Hard to test individual components
❌ Limited flexibility for custom code
❌ Opaque data flow between tasks

After (Python notebooks):

✅ Clear parameter management (widgets / arguments)
✅ Step-by-step execution with Markdown context
✅ Easy debugging with breakpoints & variable inspection
✅ Flexible fallback logic for missing modules
✅ Explicit data flow with clear variable names
✅ Improved error handling & logging
✅ Self-documenting code with business context

Migration Best-Practices Summary
--------------------------------

✅ **Do**
* Start with simple pipelines to build confidence.
* Add comprehensive Markdown documentation to notebooks.
* Implement fallback logic for missing ETL modules.
* Add extensive logging for troubleshooting.
* Test thoroughly with sample data before production.
* Use explicit function parameters instead of context properties.
* Implement data-validation checks (e.g., *apply_dqx* task).

❌ **Don’t**
* Skip testing – always validate with real data.
* Rely on context properties for internal data flow.
* Ignore performance – benchmark vs. original.
* Skip documentation – future maintainers will thank you.
* Assume every ETL module already exists – check & migrate.

Advanced YAML-Template Migration
================================

Understanding YAML-template migration
-------------------------------------

Many existing pipelines rely on declarative YAML to configure task sequences.
While convenient, they limit flexibility, hinder debugging, and block code reuse.
Migrating to Python implementations alleviates these pain points.

Typical YAML template:

.. code-block:: yaml

    # stg2brzsnowflakeingestionmergetemplate.yaml
    tasks:
      1:
        task_name: "SimpleTableReaderTask"
        _format: "delta"
        df_key: !ENV ${targettable_name}
        df_namespace: !ENV ${targetschema_name}
        tablename: !ENV ${sourcetable_name}
        schemaname: !ENV ${sourceschema_name}
        catalogname: !ENV snowflake${sourcecatalogname}
      2:
        task_name: "ApplyPysparkTask"
        dfinputnamespace: !ENV ${targetschemaname}
        dfinputkey: !ENV ${targettablename}
        dfoutputnamespace: !ENV ${targetschemaname}
        dfoutputkey: !ENV timestamp${targettable_name}
        transformations:
          - {"func": "casttimestampcolumns", "args": {"timestampformat": "timestampntz",
                                                     "isrequired": !ENV "${istimeformatnz}"}}
      # …

Limitations:

* Limited conditional logic
* Challenging debugging / tracing
* Restricted code reuse
* No compile-time type checking
* Cannot unit-test template logic directly

Benefits of Python Migration
----------------------------

Enhanced debugging
~~~~~~~~~~~~~~~~~~

.. code-block:: python

    def apply_transformations(self, df: DataFrame, context: TaskContext) -> DataFrame:
        context.logger.debug("Input schema: %s", df.schema.simpleString())
        context.logger.debug("Row count: %d", df.count())
        result_df = df.withColumn("processed_at", current_timestamp())
        context.logger.debug("Output count: %d", result_df.count())
        return result_df

Conditional logic
~~~~~~~~~~~~~~~~~

.. code-block:: python

    if source_type == "snowflake":
        df = self.read_from_snowflake(context, conf)
    elif source_type == "databricks":
        df = self.read_from_databricks(context, conf)
    else:
        raise ValueError(f"Unsupported source type: {source_type}")

Error handling
~~~~~~~~~~~~~~

.. code-block:: python

    try:
        self.write_with_upsert(df, context, conf)
    except Exception as exc:
        context.logger.warning("Upsert failed: %s – falling back to overwrite", exc)
        self.write_with_overwrite(df, context, conf)

Critical Migration Principles
----------------------------

🎯 **TOP PRIORITY: use explicit function arguments**

❌ *Bad pattern – hidden data flow via context*:

.. code-block:: python

    def execute(self, context: TaskContext, conf: Configuration) -> None:
        self._step1(context, conf)     # stores data in context
        self._step2(context, conf)     # retrieves data from context

✅ *Good pattern – explicit inputs / outputs*:

.. code-block:: python

    def execute(self, context: TaskContext, input_df: DataFrame) -> DataFrame:
        data = self.step1(context, input_df)
        result = self.step2(context, data)
        return result

Additional Resources
====================

* `Task Runner Framework <docs/taskrunnerframework.md>`_ – detailed framework docs
* `Development Standards <docs/standards.md>`_ – coding & workflow rules
* `Testing Guidelines <docs/testing/>`_ – unit / integration testing examples
* `Repository Structure <docs/repositorystructure.md>`_ – project overview

Code examples:

* Simple task migration – ``src/dataplatform/etl/data_quality/apply_dqx_rules.py``
* Data-model definition – ``src/dataplatform/data_model/bronze/swissgrid/activation_signal_picasso_swissgrid.py``
