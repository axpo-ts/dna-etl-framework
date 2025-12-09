====================
Repository Structure
====================

This document provides an overview of the DNA Platform ETL project
structure, explaining the purpose and organization of each major
directory and key files.

Quick Reference
---------------

+--------------------------------------+-------------------------------+
| Directory                            | Purpose                       |
+======================================+===============================+
| ``.devcontainer/``                   | Development container         |
|                                      | configuration for consistent  |
|                                      | environments                  |
+--------------------------------------+-------------------------------+
| ``.github/``                         | CI/CD workflows for automated |
|                                      | testing and deployment        |
+--------------------------------------+-------------------------------+
| ``_migrations/``                     | Database schema migration     |
|                                      | scripts organized by layer    |
+--------------------------------------+-------------------------------+
| ``dashboards/``                      | Databricks dashboard JSON     |
|                                      | definitions for monitoring    |
+--------------------------------------+-------------------------------+
| ``docs/``                            | Project documentation and     |
|                                      | how-to guides                 |
+--------------------------------------+-------------------------------+
| ``resources/``                       | Workflow configurations, job  |
|                                      | definitions, and templates    |
+--------------------------------------+-------------------------------+
| ``src/``                             | Python source code for the    |
|                                      | data platform                 |
+--------------------------------------+-------------------------------+
| ``tests/``                           | Unit and integration tests    |
+--------------------------------------+-------------------------------+
| ``databricks.yml``                   | Databricks Asset Bundle       |
|                                      | configuration                 |
+--------------------------------------+-------------------------------+

Project Structure Overview
--------------------------

::

   dna-platform-etl/
   ├── .devcontainer/         # Development container configuration
   ├── .github/               # CI/CD workflow definitions
   ├── _migrations/           # Database schema migration scripts
   ├── dashboards/            # Databricks dashboard JSON definitions
   ├── docs/                  # Project documentation
   │   ├── HOW_TO/            # Step-by-step guides for common tasks
   │   └── *.md               # General documentation topics
   ├── resources/             # Databricks job definitions and configurations
   │   ├── data_product/      # Data product-specific configurations
   │   ├── general/           # Shared resources
   │   └── source/            # Source system configurations
   ├── src/                   # Source code for the data platform
   │   └── data_platform/     # Core platform package
   ├── tests/                 # Unit and integration tests
   │   ├── unit/              # Unit tests
   │   └── integration/       # Integration tests
   └── databricks.yml         # Databricks bundle configuration

This project follows a multi-layered data architecture: - **Staging
layer**: Initial data landing zone - **Bronze layer**: Raw data in
standardized format - **Silver layer**: Cleansed and transformed data -
**Gold layer**: Business-ready data products

docs Directory
--------------

The ``docs`` directory contains comprehensive documentation for the
project:

The documentation is organized by topic, with specialized HOW-TO guides
for common tasks and technical reference documents for core project
components.

src Directory
-------------

The package contents live in ``src/data_platform``. The package is built
as a wheel and uploaded to Databricks on deployment. It contains all the
reusable functionality used in the workflows:

::

   src/
   ├── task_runner_notebook.py    # Entry point for Databricks notebook execution
   └── data_platform/            # Core platform package
       ├── __init__.py           # Package initialization
       ├── common.py             # Common utility functions
       ├── constants.py          # Project-wide constants and configurations
       ├── task_runner.py        # Core framework for executing data processing tasks
       ├── data/                 # Data-related functionality
       ├── notebooks/            # Databricks notebook templates
       ├── sql/                  # SQL queries and operations
       └── tasks/                # Task implementations for the data processing framework

The modular organization allows for reuse of core functionality across
different workflows while maintaining separation of concerns.

Databricks.yml
--------------

The bundle configuration file lives in the project root and is named
``databricks.yml``. This file: - Defines the project structure for
Databricks Asset Bundles - Configures environment-specific settings
(dev, test, prod) - Builds the Python wheel with PDM - Manages
deployment to target environments - Sets permissions and access control

resources Directory
-------------------

The ``resources`` directory contains all the configuration and code
needed to orchestrate the workflows. It is organized into three main
subdirectories:

::

   resources/
   ├── data_product/          # Data product-specific configurations
   │   ├── pv_forecast/
   │   └── weather/
   ├── general/               # Shared resources across products
   │   ├── data_dictionary/
   │   ├── data_quality/
   │   ├── lpi/
   └── source/                # Source system configurations
       ├── ckw/
       ├── mds/
       ├── meteomatics/
       └── volue/

This directory structure organizes: - Source-specific configurations
(for data acquisition) - Data product configurations (for analytics and
output) - General shared resources (for schema management, quality
control, etc.)

For naming conventions and file organization standards, refer to
`standards.md <./standards.md>`__

Template Files
~~~~~~~~~~~~~~

Template files define common patterns for ETL operations that can be
reused across different data sources and products.

-  **Source-specific templates**:

   ::

      resources/source/{source}/templates/

   -  Naming conventions:

      -  ``src2stg_{source}_{component}_template.yaml`` - From source to
         staging
      -  ``stg2brz_{source}_{component}_template.yaml`` - From staging
         to bronze
      -  ``brz2slv_{source}_{component}_template.yaml`` - From bronze to
         silver

   -  Example: ``stg2brz_mdlm_ingestion_merge_template.yaml``

-  **Data product templates**:

   ::

      resources/data_product/{product}/templates/

   -  Naming convention: slv2gld_{product}_{component}_template.yaml -
      From silver to gold
   -  Example: ``slv2gld_pv_forecast_template.yaml``

-  **General templates**:

   ::

      resources/general/

   -  Contains reusable configuration files:

      -  ``schema_template.yaml`` - For creating database schemas
      -  ``volume_template.yaml`` - For creating storage volumes

SQL Files
~~~~~~~~~

SQL files contain queries used for data transformation, validation, and
analysis:

::

   resources/{file_type}/{source}/sql/

Notebooks Files
~~~~~~~~~~~~~~~

Python Notebooks are used inside the workflow. They are organized by
source or product:

::

   resources/source/{source}/notebooks/
   resources/data_product/{product}/notebooks/
   resources/general/{component}/notebooks/

-  **Naming convention**:
   ``{layer_prefix}_{source}_{product}_{component}.py``

Deployment Files
~~~~~~~~~~~~~~~~

Deployment files define the orchestration workflows for data processing:

-  **Purpose**:

   -  Define job workflows and task sequences
   -  Configure webhook notifications
   -  Set permissions and scheduling
   -  Define execution parameters

-  **Location and naming**:

   ::

      resources/source/{source}/{source}_deployment.yml
      resources/data_product/{product}/{product}_deployment.yml
      resources/general/{component}/{component}_deployment.yml

-  **Example**: ``volue_timeseries_deployment.yml``

   -  Defines the workflow for Volue timeseries data processing
   -  Specifies tasks for schema creation, data ingestion, and
      transformation
   -  Sets permissions and schedules

Variables Files
~~~~~~~~~~~~~~~

Variables files contain configuration parameters used by deployment
workflows:

-  **Purpose**:

   -  Define schemas and catalog names
   -  Specify table configurations
   -  Configure data source connections
   -  Set transformation parameters

-  **Location and naming**:

   ::

      resources/source/{source}/{deployment_filename}_variables.yaml
      resources/data_product/{product}/{deployment_filename}_variables.yaml
      resources/general/{component}/{component}_variables.yaml

-  **Example**: ``volue_timeseries_deployment_variables.yaml``

   -  Defines schemas and tables for bronze and silver layers
   -  Specifies configurations for data attributes and metadata
   -  Sets processing parameters for Volue data sources

dashboards Directory
--------------------

The ``dashboards`` directory contains JSON definition files for
Databricks dashboards that provide monitoring and data quality
visualization:

::

   dashboards/
   ├── dashboards_readme.md           # Documentation for dashboards
   ├── DataQualitydev.lvdash.json     # Development environment dashboard
   ├── DataQualitypersonal_dev.lvdash.json  # Personal development dashboard
   ├── DataQualitytest.lvdash.json    # Test environment dashboard
   └── DataQualityprod.lvdash.json    # Production environment dashboard

These dashboard definitions are automatically deployed to their
respective environments during the CI/CD process.

Dashboard Components
~~~~~~~~~~~~~~~~~~~~

Each dashboard JSON file defines visualization components:

-  **Datasets**: SQL queries that fetch data for visualization
-  **Pages**: Organized sections with different focus areas (e.g., data
   quality by source)
-  **Widgets**: Visualization elements such as:

   -  Tables for detailed data inspection
   -  Bar/line charts for trend analysis
   -  Counters for KPI monitoring
   -  Filters for interactive data exploration

.devcontainer Directory
-----------------------

.devcontainer/Dockerfile
~~~~~~~~~~~~~~~~~~~~~~~~

The Dockerfile defines the custom development environment. It includes:

-  Base image: mcr.microsoft.com/devcontainers/base:ubuntu
-  Installation of various packages and tools (e.g., Python, Git, Java,
   Azure CLI, Databricks CLI)
-  Setting up locale and timezone
-  Configuring the vscode user with necessary tools and environments

.devcontainer/devcontainer.json
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

The devcontainer.json configures the development container:

-  Specifies the Dockerfile to build the container
-  Adds Visual Studio Code extensions for Python, Docker, Databricks,
   and more
-  Sets up VS Code settings such as Python interpreter path and default
   formatter
-  Runs a post-creation script (post-create.sh) to finalize setup

.github Directory
-----------------

.github/workflows/cd.yml
~~~~~~~~~~~~~~~~~~~~~~~~

The cd.yml workflow is responsible for deploying the Databricks bundle.
It performs the following tasks:

-  Initialization: Prepares environment variables in upper and lower
   case
-  Deployment:

   -  Checks out the code
   -  Sets up PDM (Python dependency manager)
   -  Logs in to Azure
   -  Installs the Databricks CLI
   -  Promotes the bundle to the specified target environment
   -  Triggers schema migrations if the target environment is not ‘dev’

.github/workflows/ci.yml
~~~~~~~~~~~~~~~~~~~~~~~~

The ci.yml workflow focuses on continuous integration and validation of
the Databricks bundle. It performs the following tasks:

-  Unit Tests: Runs unit tests using PDM
-  Linting: Runs linting and formatting checks
-  Validation:

   -  Logs in to Azure
   -  Installs the Databricks CLI
   -  Validates the Databricks bundle

-  Integration Tests: Runs integration tests and publishes results
-  Deployment: Deploys to ‘dev’, ‘test’, or ‘prod’ environments based on
   the branch

tests Directory
---------------

The ``tests`` directory contains unit and integration tests for the
project:

::

   tests/
   ├── unit/           # Unit tests for individual components
   ├── integration/    # Integration tests for end-to-end functionality
   ├── __init__.py     # Package initialization
   ├── entrypoint.py   # Test entry point
   └── pytest_databricks.py  # Pytest configuration for Databricks

Tests ensure the quality and reliability of the codebase and verify that
all components work correctly together.

--------------

\_migrations Directory
----------------------

The ``_migrations`` directory contains scripts and notebooks used to
manage database schema migrations across the different data layers.

These scripts ensure that the database schema is up-to-date with the
application’s requirements by running necessary SQL commands or
DataFrame operations.

migration_script.py
~~~~~~~~~~~~~~~~~~~

The ``migration_script.py`` is responsible for orchestrating the
migration process. It performs the following tasks:

1.  Read Parameters: Fetches necessary parameters such as catalog name,
    schema name, table name, and migration script path.
2.  Create Migration Table: Ensures the migration table exists to keep
    track of executed scripts.
3.  List All Migration Scripts: Lists all available migration scripts in
    the specified path.
4.  Read Migration Table: Reads the already executed scripts from the
    migration table.
5.  Insert Script to Migration Table: Inserts the executed script’s name
    into the migration table.
6.  Get Scripts That Already Ran: Fetches the list of already executed
    scripts.
7.  Get All Scripts: Fetches the list of all available scripts.
8.  Find New (Unrun) Scripts: Identifies scripts that have not been
    executed yet.
9.  Sort Scripts by Name: Sorts the scripts in the order of their
    execution.
10. Run the Scripts in Order: Executes the identified scripts in the
    sorted order and logs the execution.

Adding a New Migration
^^^^^^^^^^^^^^^^^^^^^^

To add a new migration, follow these steps using
``_migrations/bronze/advanced_analytics_shared/0001_rename_audit_cols_too_timeseries_latest_table.py``
as an example:

1. Create a New Script: Create a new migration script in the appropriate
   directory (e.g., ``_migrations/bronze/advanced_analytics_shared/``).
2. Script Content:

   -  Start with initializing SparkSession
   -  Fetch necessary parameters using dbutils.widgets.get
   -  Define the operations to be performed on the target table

3. Save the Script: Save the script with an appropriate name indicating
   the operation and version (e.g.,
   ``0002_add_new_column_to_table.py``).
4. Run the Migration: Execute the ``migration_script.py`` to
   automatically run the new migration script and update the migration
   table.

`Home <../README.md>`__
