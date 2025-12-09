==================
Data Quality Rules
==================

Tooling and Approach
--------------------

The application of data quality rules and the capture of reporting
statistics related to them is a bespoke festure of the dna platform
built on a library from Databricks Labs called DQX. Details of the
library can be found in the `DQX
Documentation <https://databrickslabs.github.io/dqx/docs/motivation/>`__.

Rules are stored in the data_quality_rules table in the data_quality
schema and are populated from configuration files. The
data_quality_orchestration_workflow will populate the table from
configuration files.

For rules to be applied within a workflow, the ApplyDqxRulesTask
must be added into whichever template is being used as part of
that workflow.

ApplyDqxRulesTask
-----------------

For the ApplyDqxRulesTask to function, the tables data_quality_metrics, data_quality_quarantined, and data_quality_rules must be present in the data_quality schema within the silver catalog.

Running the data_quality_orchestration_workflow will create these tables if they are missing.

If the tables are not present, the workflow will exit and display a warning message.
The ApplyDqxRulesTask is a custom task within the framework which
applies rules defined in the json data quality rules files menitoned
below. It is best used in a template when all loading or transformation
of the target dataframe has completed and is ready to be output back
to the workflow or written/merged into a delta table.

Example usage within a template:
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
.. code:: yaml

  0:
    task_name: "ApplyDqxRulesTask"
    df_input_namespace: !ENV ${target_schema_name}
    df_input_key: !ENV sql_${target_table_name}
    df_output_namespace: !ENV ${target_schema_name}
    df_output_key: !ENV dq_${target_table_name}
    target_table_name: !ENV ${target_table_name}
    target_schema_name: !ENV ${target_schema_name}
    dq_rules_table: !ENV ${dq_rules_table}
    dq_metrics_table: !ENV ${dq_metrics_table}
    dq_quarantine_table: !ENV ${dq_quarantine_table}

The following three parameters should also be copied as below into the deployment.yml
file for any task which makes use of DQ rules, no values require changing:

.. code:: yaml

  "dq_rules_table": "${var.ENV_CATALOG_IDENTIFIER}_silver.${var.ENV_SCHEMA_PREFIX}data_quality.data_quality_rules",
  "dq_metrics_table": "${var.ENV_CATALOG_IDENTIFIER}_silver.${var.ENV_SCHEMA_PREFIX}data_quality.data_quality_metrics",
  "dq_quarantine_table": "${var.ENV_CATALOG_IDENTIFIER}_silver.${var.ENV_SCHEMA_PREFIX}data_quality.data_quality_quarantined"

Example of table parameters in deployment file:
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

.. code:: yaml

  task:
    task_key: example_task_key
    notebook_task:
      notebook_path: ${workspace.root_path}/files/src/task_runner_notebook
      base_parameters:
        conf_file: ${workspace.root_path}/files/resources/source/example/templates/example.yaml
        conf_parameter: '{
          "schema_prefix": "${var.ENV_SCHEMA_PREFIX}",
          "env_catalog_identifier": "${var.ENV_CATALOG_IDENTIFIER}_",
          "target_table_name": "{{input.target_table_name}}",
          "target_schema_name": "{{input.target_schema_name}}",
          "target_catalog_name": "{{input.target_catalog_name}}",
          "source_catalog_name": "{{input.source_catalog_name}}",
          "source_schema_name": "{{input.source_schema_name}}",
          "source_table_name": "{{input.source_table_name}}",
          "dq_rules_table": "${var.ENV_CATALOG_IDENTIFIER}_silver.${var.ENV_SCHEMA_PREFIX}data_quality.data_quality_rules",
          "dq_metrics_table": "${var.ENV_CATALOG_IDENTIFIER}_silver.${var.ENV_SCHEMA_PREFIX}data_quality.data_quality_metrics",
          "dq_quarantine_table": "${var.ENV_CATALOG_IDENTIFIER}_silver.${var.ENV_SCHEMA_PREFIX}data_quality.data_quality_quarantined",
          "ordered_columns": "{{input.ordered_columns}}"
          }'
      source: WORKSPACE
    libraries:
      - whl: ../../../dist/*.whl
    job_cluster_key: job_cluster_medium


Example Data Quality Rule Configuration File
--------------------------------------------

A file can contain multiple rule definitions for multiple tables

Using Standard Platform Rule
^^^^^^^^^^^^^^^^^^^^^^^^^^^^

.. code:: json

     {
       "Schema": "market_data_and_fundamentals",
       "Table": "demo_table",
       "Name": "audit_columns_not_null",
       "Column": "created_at,created_by,updated_at,updated_by",
       "Rule": "NOT_NULL",
       "Constraint": "",
       "Tag": "valid",
       "Dimension": "CONSISTENCY",
       "Description": "Except Columns to be Not Null",
       "Enabled": true,
       "ToleranceLevel": ""
     }

Using Custom Rule
^^^^^^^^^^^^^^^^^

.. code:: json

     {
       "Schema": "market_data_and_fundamentals",
       "Table": "demo_table",
       "Name": "expect_license_not_in_range",
       "Column": "license",
       "Rule": "INHERIT",
       "Constraint": "license > 7",
       "Tag": "valid",
       "Dimension": "COMPLETENESS",
       "Description": "Except Columns to be in range",
       "Enabled": true,
       "ToleranceLevel": ""
     }

Explanation of Fields
---------------------

+-----------------------------------+-----------------------------------+
| Column                            | Description                       |
+===================================+===================================+
| Column                            | Column for which the rules are to |
|                                   | be applied. If multiple columns   |
|                                   | then delimit them with commas     |
+-----------------------------------+-----------------------------------+
| Constraint                        | (optional) SQL Expression for     |
|                                   | defining custom rules             |
|                                   | e.g. ‘license > 7’, this can be   |
|                                   | left blank if a standard Rule     |
|                                   | such as ‘NOT NULL’ has been       |
|                                   | defined in the Rule field         |
+-----------------------------------+-----------------------------------+
| Description                       | Descriptive statement for the     |
|                                   | Rule                              |
+-----------------------------------+-----------------------------------+
| Dimension                         | Category of the Rule              |
+-----------------------------------+-----------------------------------+
| Enabled                           | Whether to be Enabled/Disabled    |
+-----------------------------------+-----------------------------------+
| Name                              | Name of the Rule which should be  |
|                                   | descriptive                       |
+-----------------------------------+-----------------------------------+
| Rule                              | For standard functions from the   |
|                                   | platform e.g. ‘NOT NULL’ or       |
|                                   | ‘VALID_TIMESTAMP_FORMAT’. If      |
|                                   | using custom SQL Expression in    |
|                                   | the Constraint field then set to  |
|                                   | ‘INHERIT’                         |
+-----------------------------------+-----------------------------------+
| Schema                            | Schema Name in Catalog            |
+-----------------------------------+-----------------------------------+
| Table                             | Table Name within the Schema      |
+-----------------------------------+-----------------------------------+
| Tag                               | Whether it’s Valid or Not         |
+-----------------------------------+-----------------------------------+
| Tolerance Level                   | To Tolerate the Rule              |
+-----------------------------------+-----------------------------------+


Creating a json Data Quality Rules File
---------------------------------------

To add data quality rules for a new table, follow these steps:

1. Create a json file using the template below
2. Add a new Rules and/or Constraints
3. Upload file to
   /Volumes/dna_[*environment*]_staging/data_quality/dq_files
4. Run the data_quality_orchestration_workflow to load rules into the
   data_quality_rules table
4. Rules will be applied the next time a workflow is run

Template for creating json configuration file
---------------------------------------------

.. code:: json

     {
       "Schema": "",
       "Table": "",
       "Name": "",
       "Column": "",
       "Rule": "",
       "Constraint": "",
       "Tag": "",
       "Dimension": "",
       "Description": "",
       "Enabled": true,
       "ToleranceLevel": ""
     }

Checklist for adding rules to a workflow
----------------------------------------
1. Check template to see if ApplyDqxRulesTask is already present
2. If not, add the ApplyDqxRulesTask to the template
3. Check the deployment.yml file to ensure the three DQ tables parameters are
   present and correct
4. Follow steps to create DQ rules file above

Notes
-----

The data quality rules use SQL expressions to define the conditions.
Ensure that the SQL expressions are valid and correctly reference the
columns in the source table.

`Overview <https://mingle.axpo.com/display/DAT/Data+Quality+Rules+Overview>`__

`Home <../README.md>`__
