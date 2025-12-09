===========
Development
===========

This section covers development practices, standards, and ETL implementation approaches for the DNA Platform.

General Development
===================

.. toctree::
   :maxdepth: 2
   :caption: General Development:

   development/repository_structure
   development/standards
   development/testing
   development/hotfix_release_process
   development/ci_cd

ETL Development - New Setup & migration
=======================================

The DNA Platform ETL is transitioning from a legacy task runner framework to a new modular ETL package structure.
Use the migration guide below to understand the transition and adopt the new approach.

.. toctree::
   :maxdepth: 2
   :caption: ETL New Setup:

   development/etl_refactor_and_migration
   development/data_model_definition
   development/etl_package_guidelines
   development/data_ingestion


ETL Development - Legacy Task Runner (Old Setup)
================================================

.. important::
   The task runner framework is legacy and being phased out.
   For new ETL development, use the **New Setup** (data model definition and ETL package guidelines) above.

.. toctree::
   :maxdepth: 2
   :caption: Legacy Task Runner Framework:

   development/task_runner_framework

Data Platform Operations
========================

.. toctree::
   :maxdepth: 2
   :caption: Data Operations:

   development/data_dictionary_process
   development/data_quality
   development/lpi_permissions
   development/sources
