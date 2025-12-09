=========
Standards
=========

This document outlines the standards and conventions used across the DNA
Platform ETL project to ensure consistency and maintainability.

Workflow Standards
------------------

Parameter Conventions
~~~~~~~~~~~~~~~~~~~~~

-  Parameters must be consistent across source systems:

   -  Use ``resource_id`` instead of ``id`` or ``curve_name``
   -  Include ``table`` as a parameter for loop operations
   -  Include ``buffer_date`` as a standard parameter
   -  Use ``to_date`` and ``from_date`` in each task

Orchestration Patterns
~~~~~~~~~~~~~~~~~~~~~~

-  Move related workflows to a single orchestrator flow
-  Always use ``foreach`` loops for iterating through collections
-  Use consistent job naming:

   -  Pattern: ``{layer}_{sourcename}_{operation}``
   -  Operations: ``append``, ``merge``, or ``overwrite``
   -  Example: ``bronze_meteomatics_merge``

File Structure and Naming Conventions
-------------------------------------

Layer Prefixes
~~~~~~~~~~~~~~

All files in the project should follow standardized naming conventions
using appropriate layer prefixes:

======= ======= ===============================
Layer   Prefix  Description
======= ======= ===============================
Staging ``stg`` Initial data landing zone
Bronze  ``brz`` Raw data in standardized format
Silver  ``slv`` Cleansed and transformed data
Gold    ``gld`` Business-ready data products
======= ======= ===============================

Cross-Layer Processes
~~~~~~~~~~~~~~~~~~~~~

For processes that move data between layers, use these combined
prefixes:

================= =========== ====================================
Process           Prefix      Description
================= =========== ====================================
Source to Staging ``src2stg`` Initial data ingestion
Staging to Bronze ``stg2brz`` Standardization of raw data
Bronze to Silver  ``brz2slv`` Data transformation and cleansing
Silver to Gold    ``slv2gld`` Business-ready data product creation
================= =========== ====================================

Template Files
~~~~~~~~~~~~~~

Template files define reusable patterns for ETL operations and must
follow this naming pattern:

::

   {layer_prefix}_{source}_{component}_template.yaml

Examples: - ``stg2brz_mdlm_ingestion_merge_template.yaml`` -
``brz2slv_volue_timeseries_template.yaml`` -
``slv2gld_pv_forecast_template.yaml``

Configuration Files
~~~~~~~~~~~~~~~~~~~

Configuration files contain parameters needed for specific components
and are organized by source or product:

=============== ==========================================
Type            Location Path
=============== ==========================================
Source-specific ``resources/source/{source}/conf/``
Data product    ``resources/data_product/{product}/conf/``
General/shared  ``resources/general/{component}/conf/``
=============== ==========================================

SQL Files
~~~~~~~~~

SQL query files must be organized according to their purpose and
associated component:

=============== =========================================
Type            Location Path
=============== =========================================
Source-specific ``resources/source/{source}/sql/``
Data product    ``resources/data_product/{product}/sql/``
General/shared  ``resources/general/{component}/sql/``
=============== =========================================

Notebooks
~~~~~~~~~

Executable notebooks that implement workflow steps should be organized
as follows:

=============== ===============================================
Type            Location Path
=============== ===============================================
Source-specific ``resources/source/{source}/notebooks/``
Data product    ``resources/data_product/{product}/notebooks/``
General/shared  ``resources/general/{component}/notebooks/``
=============== ===============================================

Deployment Files
~~~~~~~~~~~~~~~~

Deployment files define complete orchestration workflows and follow this
pattern:

::

   {source}_deployment.yml
   {product}_deployment.yml

These files are stored in the appropriate source or product directory: -
resources/source/{source}/{source}_deployment.yml -
resources/data_product/{product}/{product}_deployment.yml -
resources/general/{component}/{component}_deployment.yml

Variables Files
~~~~~~~~~~~~~~~

Variables files contain configuration parameters for deployments and
follow this pattern:

::

   {deployment_filename}_variables.yaml

These files are stored alongside their corresponding deployment files: -
resources/source/{source}/{deployment_filename}_variables.yaml -
resources/data_product/{product}/{deployment_filename}_variables.yaml -
resources/general/{component}/{deployment_filename}_variables.yaml

Code Standards
--------------

Python Code Standards
~~~~~~~~~~~~~~~~~~~~~

For detailed Python coding standards, refer to: -
`PYTHON_CODING_STANDARDS <./PYTHON_CODING_STANDARDS.md>`__

Key Python requirements: - Use type hints for all function parameters
and return values - Follow PEP 8 style guidelines - Document all classes
and functions with docstrings - Organize imports by standard library,
third-party, and local modules

PySpark Code Standards
~~~~~~~~~~~~~~~~~~~~~~

For detailed PySpark coding standards, refer to: -
`PYSPARK_CODING_STANDARDS <./PYSPARK_CODING_STANDARDS.md>`__

Key PySpark requirements: - Prefer DataFrame operations over RDDs - Use
appropriate caching strategies - Include column types in schema
definitions - Use optimization techniques for large dataset operations

Testing Standards
-----------------

Testing is a critical part of ensuring data quality and reliable ETL
processes. The following standards should be followed for all project
tests:

Test Coverage Requirements
~~~~~~~~~~~~~~~~~~~~~~~~~~

-  **Unit Tests**:

   -  Required for each Task class and core function
   -  Focus on testing individual components in isolation
   -  Located in ``tests/unit/`` directory

-  **Integration Tests**:

   -  Required for critical workflows
   -  Test multiple components working together
   -  Located in ``tests/integration/`` directory

-  **End-to-End Tests**:

   -  Recommended for validating complete workflows
   -  Verify that entire processes work correctly
   -  Should include configuration validation

-  **Acceptance Tests**:

   -  Required for key business requirements
   -  Validate business logic using real production data
   -  Focus on data quality and business outcomes

Testing Best Practices
~~~~~~~~~~~~~~~~~~~~~~

-  **Test Data Management**:

   -  Use CSV files for test data
   -  Store test fixtures in the appropriate test directory
   -  Use meaningful file names that indicate test purpose

-  **Test Structure**:

   -  Create reusable dataframes in conftest.py
   -  Organize tests by component/module
   -  Follow the Arrange-Act-Assert pattern

-  **Test Execution**:

   -  Tests must pass before merging code
   -  Run appropriate tests locally before committing
   -  CI pipeline will run all tests automatically

Documentation Standards
-----------------------

Code Documentation
~~~~~~~~~~~~~~~~~~

-  All Python files must include a header comment explaining the file’s
   purpose
-  All classes must include docstrings explaining their responsibility
-  All public methods must include docstrings with:

   -  Description of functionality
   -  Parameter descriptions
   -  Return value description
   -  Examples where appropriate

Markdown Documentation
~~~~~~~~~~~~~~~~~~~~~~

-  All documentation in ``/docs`` must be written in Markdown
-  Include navigation links at top and bottom of each document
-  Use proper heading hierarchies (# for title, ## for sections, etc.)
-  Include code examples where appropriate
-  Link to related documentation for cross-references

`Home <../README.md>`__
