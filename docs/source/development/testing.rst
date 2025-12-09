=======
Testing
=======

Configure databricks connect
------------

https://docs.databricks.com/en/dev-tools/databricks-connect/python/index.html

.. code:: bash

   databricks auth login --configure-cluster --host https://adb-4027174922259360.0.azuredatabricks.net/

.. code:: bash
   ✔ Databricks profile name: personal_dev█
   Please open https://adb-4027174922259360.0.azuredatabricks.net/....
   Configured cluster: Shared (0410-134943-bx7aando)

-  Add profile name as personal_dev
-  Click on the link to authenticate
-  Select the shared cluster

.. code:: bash
   export DATABRICKS_HOST=https://adb-4027174922259360.0.azuredatabricks.net/
   export DATABRICKS_TOKEN=********-2
   export DATABRICKS_CLUSTER_ID=0410-134943-bx7aando
   export DATABRICKS_WORKSPACE_ORG_ID=4027174922259360


-  Set up environment variables if not using the extension in Vscode - https://learn.microsoft.com/en-us/azure/databricks/dev-tools/vscode-ext/

Unit testing
------------

-  test functions
-  test classes where there is no dependency on databricks or unity
   catalog

.. code:: bash

   pdm unit_test

To create a new unit test, follow these steps:
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

By following these steps and using tests/unit/test_schema_tasks.py as an
example, you can create new unit tests for your project. Make sure to
adapt the test configurations and assertions according to the specific
requirements of the functionality you are testing.

1. Import necessary modules: Ensure you import all the required modules
   at the beginning of your test file. For example:

.. code:: python

   import logging
   from collections.abc import Generator
   import pytest
   from data_platform import constants
   from data_platform.tasks.schema_tasks import CreateSchemaTask
   from data_platform.tasks.task_utility import Configuration, TaskContext
   from pyspark.sql import DataFrame, SparkSession

2. Define fixtures: Use @pytest.fixture to set up any necessary
   preconditions for your tests. For example:

.. code:: python

   @pytest.fixture()
   def setup_metadata_table(spark: SparkSession) -> Generator:
       spark.sql("CREATE SCHEMA IF NOT EXISTS test")
       spark.sql(f"CREATE TABLE IF NOT EXISTS test.metadata_mocked ({constants.metadata_table_schema})")
       df = spark.sql("SELECT * FROM test.metadata_mocked")
       yield df
       spark.sql("DROP SCHEMA test CASCADE")

3. Write test functions: Define your test functions using the def
   keyword. Use assertions to validate the expected outcomes. Example:

.. code:: python

   def test_create_schema_task(spark: SparkSession, logger: logging.Logger) -> None:
       logger.info("Testing the Create Schema task")

       test_etl_config = Configuration(
           {
               "1": {
                   "task_name": "CreateSchemaTask",
                   "schema_name": "test_schema",
               }
           }
       )
       logger.info(test_etl_config)
       task_context = TaskContext(logger=logger, spark=spark, configuration=test_etl_config)
       etl_job = CreateSchemaTask()
       etl_job.execute(context=task_context, conf=test_etl_config.get_tree("1"))

       df = spark.sql("SHOW SCHEMAS")
       assert df.filter("namespace = 'test_schema'").count() == 1

4. Run your tests: Use the following command to run your unit tests:

.. code:: bash

   pdm unit_test

Integration testing
-------------------

-  main method calls
-  test pipelines (yaml pipelines)
-  materialized views
-  dlt pipelines where there is a dependency on databricks or unity
   catalog


To start the integration tests:

-  In VS Code
-  Go to Run on the top tool bar
-  Click Start Debugging

To create a new integration test, follow these steps:
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

By following these steps and using
tests/integration/test_databricks_tasks.py as an example, you can create
new integration tests for your project. Make sure to adapt the test
configurations and assertions according to the specific requirements of
the functionality you are testing.

1. Import necessary modules: Ensure you import all the required modules
   at the beginning of your test file. For example:

.. code:: python

   import logging
   import random
   import string
   from collections.abc import Generator

   import pytest
   from data_platform.tasks.schema_tasks import CreateVolumeTask
   from data_platform.tasks.task_utility import Configuration, TaskContext
   from data_platform.tasks.writer_tasks import JsonToVolumeWriterTask
   from pyspark.sql import SparkSession

2. Define helper functions: Create any helper functions you might need
   within your tests. For example, a function to generate random IDs:

.. code:: python

   def id_generator(size: int = 6, chars: str = string.ascii_uppercase + string.digits) -> str:
       return "".join(random.choice(chars) for _ in range(size))

3. Define fixtures: Use @pytest.fixture to set up any necessary
   preconditions for your tests. For example:

.. code:: python

   @pytest.fixture()
   def setup_test_schema(spark: SparkSession) -> Generator:
       random_id = id_generator()
       schema_name = f"{random_id}_test".lower()
       spark.sql(f"CREATE SCHEMA IF NOT EXISTS dna_dev_staging.{schema_name}")
       yield schema_name
       spark.sql(f"DROP SCHEMA dna_dev_staging.{schema_name} CASCADE")

4. Write test functions: Define your test functions using the def
   keyword. Use assertions to validate the expected outcomes. Example:

.. code:: python

   def test_write_json_to_volume_with_attributes(spark: SparkSession, logger: logging.Logger, setup_test_schema: str) -> None:
       logging.info("Testing the JsonToVolumeWriterTask")
       test_etl_config = Configuration(
           {
               "1": {
                   "task_name": "CreateVolumeTask",
                   "schema_name": setup_test_schema,
                   "catalog_name": "dna_dev_staging",
                   "volume_name": "test",
                   "comment": "test",
               }
           }
       )
       task_context = TaskContext(logger=logger, spark=spark, configuration=test_etl_config)
       etl_job = CreateVolumeTask()
       etl_job.execute(context=task_context, conf=test_etl_config.get_tree("1"))
       result = spark.sql(f"DESCRIBE VOLUME dna_dev_staging.{setup_test_schema}.test")
       assert result.count() == 1

       # Write json object to volume
       test_etl_config = Configuration(
           {
               "1": {
                   "task_name": "JsonToVolumeWriterTask",
                   "schema_name": setup_test_schema,
                   "catalog_name": "dna_dev_staging",
                   "volume_name": "test",
                   "file_name": "test",
                   "df_input_namespace": "test",
                   "df_input_key": "test",
                   "add_timestamp": "True",
               }
           }
       )
       task_context = TaskContext(logger=logger, spark=spark, configuration=test_etl_config)
       task_context.put_property("test", "test", '{ "name":"John", "age":30, "city":"New York"}')
       etl_job_json = JsonToVolumeWriterTask()
       etl_job_json.execute(context=task_context, conf=test_etl_config.get_tree("1"))

       count_result = spark.sql(f"LIST 'dbfs:/Volumes/dna_dev_staging/{setup_test_schema}/test/'").count()
       assert count_result == 1

`Home <../README.md>`__
