==================================
How to add a new file based Source
==================================

How to Create a New Config in Bronze for an file source
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

This guide provides step-by-step instructions on how to create a new
configuration using the
resources/source/ebx/templates/stg2brz_ebx_api_merge_template.yaml for
file ingestion within the axpo-ts/dna-platform-etl repository.

-  resources/source/ebx/templates/stg2brz_ebx_api_merge_template.yaml

Step:
^^^^^

1. Create a volume for Autoloader checkpoints.
2. Read stream data.
3. Flatten the data (optional mostly due to source file) and add audit
   columns.
4. Create a table to write the output data.
5. Merge data into the target table (only updated changed rows).

.. code:: yaml

   # Pipeline for flattening the EBX API data and writing it to bronze layer
   #
   # Steps:
   # 1. Create volume for Autoloader checkpoints
   # 2. Read stream
   # 3. Flatten the data & add audit columns
   # 4. Create table where to write the output data
   # 5. Merge data into target table (only updated changed rows)

   tasks:
     1:
       task_name: "CreateVolumeTask"
       schema_name: !ENV ${schema_prefix}${source_schema_name}
       catalog_name: !ENV ${env_catalog_identifier}${source_catalog_name}
       volume_name: !ENV ${volume_name}
       comment: !ENV ${volume_comment}
     2:
       task_name: "SimpleStreamingFileReaderTask"
       _format: "cloudFiles"
       df_key: !ENV autoloader_${target_table_name}
       df_namespace: !ENV ${target_schema_name}
       reader_options:
         inferSchema: True
         cloudFiles.format: !ENV ${source_file_format}
         cloudFiles.schemaLocation: !ENV "/Volumes/${env_catalog_identifier}${source_catalog_name}/${schema_prefix}${source_schema_name}/${volume_name}/checkpoint/schemaLocation"
         cloudFiles.schemaEvolutionMode: !ENV ${schema_evolution_mode}
         cloudFiles.schemaHints: !ENV ${schema}
         cloudFiles.maxFileAge: !ENV ${max_file_age}
         cloudFiles.maxFilesPerTrigger: 1
         forceDeleteTempCheckpointLocation: "true"
         path: !ENV /Volumes/${env_catalog_identifier}${source_catalog_name}/${schema_prefix}${source_schema_name}/${volume_name}
         pathGlobfilter: !ENV "${source_filter}"
     3:
       task_name: "ApplyPysparkTask"
       df_input_namespace: !ENV ${target_schema_name}
       df_input_key: !ENV autoloader_${target_table_name}
       df_output_namespace: !ENV ${target_schema_name}
       df_output_key: !ENV sql_${target_table_name}
       transformations:
         - { "func": "explode_rows", "args": { "explode_col": "rows" } }
         - {
             "func": "select_cols",
             "args": { "select_statement": "_rows.content.*" },
           }
         - { "func": "flatten_content", "args": {} }
         - { "func": "drop_cols", "args": { "drop_cols": ["TechnicalFields"] } }
         - { "func": "sanitize_columns", "args": {} }
         - { "func": "add_audit_cols_snake_case", "args": {} }
     4:
       task_name: "CreateTableFromDataFrameTask"
       table_name: !ENV ${target_table_name}
       schema_name: !ENV ${schema_prefix}${target_schema_name}
       catalog_name: !ENV ${env_catalog_identifier}${target_catalog_name}
       df_key: !ENV sql_${target_table_name}
       df_namespace: !ENV ${target_schema_name}
       writer_options:
         primary_keys: !ENV ${primary_keys}
     5:
       task_name: "SimpleStreamingUpsertChangesTask"
       table_name: !ENV ${target_table_name}
       schema_name: !ENV ${schema_prefix}${target_schema_name}
       catalog_name: !ENV ${env_catalog_identifier}${target_catalog_name}
       df_key: !ENV sql_${target_table_name}
       df_namespace: !ENV ${target_schema_name}
       primary_keys: !ENV "${primary_keys}"
       trigger_kwargs: { "availableNow": true }
       writer_options:
         checkpointLocation: !ENV "/Volumes/${env_catalog_identifier}${source_catalog_name}/${schema_prefix}${source_schema_name}/${volume_name}/checkpoint/_checkpoint"
         mergeSchema: "true"
     6:
       task_name: "CreateTableTagsTask"
       table_name: !ENV ${target_table_name}
       schema_name: !ENV ${schema_prefix}${target_schema_name}
       catalog_name: !ENV ${env_catalog_identifier}${target_catalog_name}
       tags:
         License: Non licence
         Confidentiality Level: C3_Confidential
         Personal sensitive information: No PII
     7:
       task_name: "CreateTableCommentTask"
       table_name: !ENV ${target_table_name}
       schema_name: !ENV ${schema_prefix}${target_schema_name}
       catalog_name: !ENV ${env_catalog_identifier}${target_catalog_name}
       comment: "Default"

How to use that config from a Databricks workflow
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

This guide provides step-by-step instructions on how to use the
configuration file
resources/source/ebx/templates/stg2brz_ebx_api_merge_template.yaml
within the resource file resources/data_source/ebx/ebx_deployment.yml.

Overview of conf/staging/ebx/api_to_volume.yml
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

The configuration file
resources/source/ebx/templates/stg2brz_ebx_api_merge_template.yaml
defines a series of tasks to create a volume, read and process data from
a stream, and write the processed data to a target table. Below is the
structure of the file:

.. code:: yaml

   # Pipeline for flattening the EBX API data and writing it to bronze layer
   #
   # Steps:
   # 1. Create volume for Autoloader checkpoints
   # 2. Read stream
   # 3. Flatten the data & add audit columns
   # 4. Create table where to write the output data
   # 5. Merge data into target table (only updated changed rows)

   tasks:
     1:
       task_name: "CreateVolumeTask"
       schema_name: !ENV ${schema_prefix}${source_schema_name}
       catalog_name: !ENV ${env_catalog_identifier}${source_catalog_name}
       volume_name: !ENV ${volume_name}
       comment: !ENV ${volume_comment}
     2:
       task_name: "SimpleStreamingFileReaderTask"
       _format: "cloudFiles"
       df_key: !ENV autoloader_${target_table_name}
       df_namespace: !ENV ${target_schema_name}
       reader_options:
         inferSchema: True
         cloudFiles.format: !ENV ${source_file_format}
         cloudFiles.schemaLocation: !ENV "/Volumes/${env_catalog_identifier}${source_catalog_name}/${schema_prefix}${source_schema_name}/${volume_name}/checkpoint/schemaLocation"
         cloudFiles.schemaEvolutionMode: !ENV ${schema_evolution_mode}
         cloudFiles.schemaHints: !ENV ${schema}
         cloudFiles.maxFileAge: !ENV ${max_file_age}
         cloudFiles.maxFilesPerTrigger: 1
         forceDeleteTempCheckpointLocation: "true"
         path: !ENV /Volumes/${env_catalog_identifier}${source_catalog_name}/${schema_prefix}${source_schema_name}/${volume_name}
         pathGlobfilter: !ENV "${source_filter}"
     3:
       task_name: "ApplyPysparkTask"
       df_input_namespace: !ENV ${target_schema_name}
       df_input_key: !ENV autoloader_${target_table_name}
       df_output_namespace: !ENV ${target_schema_name}
       df_output_key: !ENV sql_${target_table_name}
       transformations:
         - { "func": "explode_rows", "args": { "explode_col": "rows" } }
         - {
             "func": "select_cols",
             "args": { "select_statement": "_rows.content.*" },
           }
         - { "func": "flatten_content", "args": {} }
         - { "func": "drop_cols", "args": { "drop_cols": ["TechnicalFields"] } }
         - { "func": "sanitize_columns", "args": {} }
         - { "func": "add_audit_cols_snake_case", "args": {} }
     4:
       task_name: "CreateTableFromDataFrameTask"
       table_name: !ENV ${target_table_name}
       schema_name: !ENV ${schema_prefix}${target_schema_name}
       catalog_name: !ENV ${env_catalog_identifier}${target_catalog_name}
       df_key: !ENV sql_${target_table_name}
       df_namespace: !ENV ${target_schema_name}
       writer_options:
         primary_keys: !ENV ${primary_keys}
     5:
       task_name: "SimpleStreamingUpsertChangesTask"
       table_name: !ENV ${target_table_name}
       schema_name: !ENV ${schema_prefix}${target_schema_name}
       catalog_name: !ENV ${env_catalog_identifier}${target_catalog_name}
       df_key: !ENV sql_${target_table_name}
       df_namespace: !ENV ${target_schema_name}
       primary_keys: !ENV "${primary_keys}"
       trigger_kwargs: { "availableNow": true }
       writer_options:
         checkpointLocation: !ENV "/Volumes/${env_catalog_identifier}${source_catalog_name}/${schema_prefix}${source_schema_name}/${volume_name}/checkpoint/_checkpoint"
         mergeSchema: "true"
     6:
       task_name: "CreateTableTagsTask"
       table_name: !ENV ${target_table_name}
       schema_name: !ENV ${schema_prefix}${target_schema_name}
       catalog_name: !ENV ${env_catalog_identifier}${target_catalog_name}
       tags:
         License: Non licence
         Confidentiality Level: C3_Confidential
         Personal sensitive information: No PII
     7:
       task_name: "CreateTableCommentTask"
       table_name: !ENV ${target_table_name}
       schema_name: !ENV ${schema_prefix}${target_schema_name}
       catalog_name: !ENV ${env_catalog_identifier}${target_catalog_name}
       comment: "Default"

Integration into resources/source/ebx/deployment.yml
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

The resource file resources/data_source/ebx/ebx_deployment.yml contains
job definitions and tasks that reference the configuration file
conf/staging/ebx/api_to_volume.yml. Below is an excerpt showing how the
configuration file is utilized:

.. code:: yaml

   resources:
     jobs:
       ebx_orchestration_workflow:
         name: ebx_orchestration_workflow_${bundle.target}

         webhook_notifications:
           on_failure:
             - id: ${var.slack_alert_notification}
             - id: ${var.teams_alert_notifications}

         notification_settings:
           no_alert_for_skipped_runs: true
           no_alert_for_canceled_runs: true

         permissions:
           - group_name: CL-AZ-SUBS-dna-${var.azure_subscription_env}-central-contributor
             level: CAN_MANAGE_RUN

         schedule:
           quartz_cron_expression: 32 0 0 ? * Sun
           timezone_id: UTC
           pause_status: ${var.schedule_status}

         parameters:
           - name: target_snapshot_column_or_date
             default: "updated_at"

         tasks:
           - task_key: get_variables
             notebook_task:
               notebook_path: ${workspace.root_path}/files/src/data_platform/notebooks/get_variables
               base_parameters:
                 variable_file: ${workspace.root_path}/files/resources/source/ebx/variables.yaml
               source: WORKSPACE
             job_cluster_key: job_cluster

           - task_key: schema_creation
             depends_on:
               - task_key: get_variables
             for_each_task:
               inputs: "{{tasks.get_variables.values.schemas}}"
               concurrency: 5
               task:
                 task_key: schema_creation_iteration
                 job_cluster_key: job_cluster
                 notebook_task:
                   notebook_path: ${workspace.root_path}/files/src/task_runner_notebook
                   base_parameters:
                     conf_file: ${workspace.root_path}/files/resources/general/schema_creation/templates/schema_template.yaml
                     conf_parameter: '{"ENV_SCHEMA_PREFIX": "${var.ENV_SCHEMA_PREFIX}", "ENV_CATALOG_IDENTIFIER": "${var.ENV_CATALOG_IDENTIFIER}", "schema_name": "{{input.schema_name}}", "catalog_name": "{{input.catalog_name}}"}'
                   source: WORKSPACE
                 libraries:
                   - whl: ../../../dist/*.whl

           - task_key: bronze_ebx
             depends_on:
               - task_key: staging_ebx
             for_each_task:
               inputs: "{{tasks.get_variables.values.bronze}}"
               concurrency: 6
               task:
                 task_key: bronze_ebx_iteration
                 notebook_task:
                   notebook_path: ${workspace.root_path}/files/src/task_runner_notebook
                   base_parameters:
                     conf_file: ${workspace.root_path}/files/resources/source/ebx/templates/stg2brz_ebx_api_merge_template.yaml
                     conf_parameter: '{
                       "volume_name": "{{input.table}}",
                       "volume_comment": "Volume to store files for EBX API dataset `{{input.table}}`.",
                       "source_schema_name": "ebx",
                       "schema_prefix": "${var.ENV_SCHEMA_PREFIX}",
                       "env_catalog_identifier": "${var.ENV_CATALOG_IDENTIFIER}_",
                       "source_catalog_name": "staging",
                       "target_table_name":  "{{input.table}}",
                       "target_schema_name": "ebx",
                       "target_catalog_name": "bronze",
                       "source_filter": "*.json",
                       "output_mode": "append",
                       "max_file_age": "90 days",
                       "source_file_format": "json",
                       "schema_evolution_mode": "addNewColumns",
                       "schema": "{{input.schema}}",
                       "primary_keys": "{{input.primary_keys}}"
                       }'
                   source: WORKSPACE
                 libraries:
                   - whl: ../../../dist/*.whl
                 job_cluster_key: job_cluster

Step-by-Step Guide
^^^^^^^^^^^^^^^^^^

1. Create Variables File:

Create a variables file at
${workspace.root_path}/files/resources/source/ebx/variables.yaml with
the required parameters.

**Variable file**

.. code:: yaml

   schemas:
     - schema_name: ebx
       catalog_name: bronze
     - schema_name: reference
       catalog_name: silver

   staging:
     - table: commodity
       api_url: Commodity?pageSize=10000

   bronze:
     - table: commodity
       schema: "pagination STRUCT<firstPage: STRING, lastPage: STRING, nextPage: STRING, previousPage: STRING>,rows ARRAY<STRUCT<content: STRUCT<CommodityId: STRUCT<content: BIGINT>, CommodityName: STRUCT<content: STRING>, isRelevantForAgreements: STRUCT<content: BOOLEAN>, isRelevantForEsContact: STRUCT<content: BOOLEAN>, isRelevantForLegalEntities: STRUCT<content: BOOLEAN>, isRelevantForMandateMarket: STRUCT<content: BOOLEAN>>, details: STRING, label: STRING>>"
       primary_keys: CommodityId

2. Define Tasks:

Define tasks in the resources/data_source/ebx/ebx_deployment.yml file to
reference the configuration file
resources/source/ebx/templates/stg2brz_ebx_api_merge_template.yaml.

3. Configure Parameters:

Configure the parameters in the base_parameters section to use variables
and values specific to your deployment.

4. Set Up Task Dependencies:

Ensure that task dependencies are correctly set up to execute the tasks
in the desired order inside the resource yaml file.

.. code:: yaml

    - task_key: bronze_ebx
       depends_on:
         - task_key: staging_ebx

5. Run the Workflow:

Trigger the workflow to execute the tasks defined in
resources/data_source/ebx/ebx_deployment.yml.

.. code:: bash

      databricks bundle deploy --target personal_dev --profile dev

.. code:: bash

      databricks bundle run --target personal_dev --profile dev

select the name of the new workflow
