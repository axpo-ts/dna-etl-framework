===================
PySpark Style Guide
===================

PySpark provides you with access to Python language bindings to the
Apache Spark big data engine.

This document outlines the best practices you should follow when writing
PySpark code.

Automatic Python code formatting tools already exist so this document
focuses specifically on PySpark best practices and how to structure
PySpark code, not on general Python code formatting.

PySpark Coding Standards
========================

General Guidelines
------------------

-  Follow PEP 8 for Python code style.
-  Use meaningful names for variables, functions, and classes.
-  Write docstrings for all public modules, functions, classes, and
   methods.
-  Keep functions small and focused on a single task.

Imports
-------

-  Group imports into three categories: standard library, third-party
   packages, and local modules.
-  Use absolute imports.
-  Import specific classes or functions instead of the entire module,
   when possible.

.. code:: python

   # Standard Library
   from datetime import datetime

   # Third-Party Packages
   import pandas as pd
   from pyspark.sql import DataFrame

   # Local Modules
   from data_platform.common import get_dbutils

Import the PySpark SQL functions into a variable named ``F`` to avoid
polluting the global namespace.

.. code:: python

   from pyspark.sql import functions as F

You can structure libraries such that the public interface is exposed in
a single namespace, so its easy to identify the source of subsequent
function invocations. Here’s an example with the `quinn
library <https://github.com/MrPowers/quinn>`__:

.. code:: python

   import quinn

   quinn.validate_absence_of_columns(df, ["age", "cool"])

This import style makes it easy to identify where the
``validate_absence_of_columns`` function was defined.

You can also use this import style:

.. code:: python

   from quinn import validate_absence_of_columns

Don’t follow this import style that makes it hard to determine where
``validate_absence_of_columns`` comes from:

.. code:: python

   from quinn import *

DataFrames
----------

-  Use meaningful names for DataFrames.
-  Chain DataFrame transformations for readability.
-  Use the select method to rename columns and select specific columns.

.. code:: python

   df = spark.read.format("csv").load("data.csv")
   df = df.selectExpr("column1 as col1", "column2 as col2")

Configuration
-------------

-  Use dataclasses for configuration.
-  Document all attributes of the configuration class.

.. code:: python

   @dataclass
   class SimpleReaderConfig:
       task_name: str
       format: str
       df_key: str
       df_namespace: str
       path: str | None = None
       table_name: str | None = None
       schema_name: str | None = None
       catalog_name: str | None = None
       reader_options: dict[str, str] = field(default_factory=dict)

Task Execution
--------------

-  Log the start and end of tasks.
-  Validate and map configurations to dataclasses.
-  Use the context object to store and retrieve DataFrames.

.. code:: python

   class SimpleBatchReaderTask(ETLTask):
       task_name = "SimpleBatchReaderTask"
       dataclass = SimpleReaderConfig

       def execute(self, context: TaskContext, conf: Configuration) -> None:
           context.logger.info("running SimpleBatchReaderTask")
           _conf = self.dataclass(**conf.as_dict())
           context.logger.info(_conf)

           df = context.spark.read.options(**_conf.reader_options).format(_conf.format).table(_conf.table_name)
           context.put_property(namespace=_conf.df_namespace, key=_conf.df_key, value=df)

Handling API Calls
------------------

-  Use httpx for asynchronous HTTP requests.
-  Handle HTTP errors and log relevant information.
-  Parse JSON responses and handle errors gracefully.

.. code:: python

   class RequestsApiReaderTask(ETLTask):
       task_name = "RequestsApiReaderTask"
       timeout = 0
       verify = False

       def execute(self, context: TaskContext, conf: Configuration) -> None:
           context.logger.info(f"running {self.task_name}")
           _conf = self.parse_config(context, conf)
           response = self.execute_request(context, _conf, {})
           content = self.extract_content(context, _conf, response)
           context.put_property(namespace=_conf.df_namespace, key=_conf.df_key, value=content)

       def execute_request(self, context: TaskContext, conf: BaseApiReaderConfig, payload: dict) -> Response:
           response = requests.get(conf.api_url, params=payload, headers={}, timeout=self.timeout, verify=self.verify)
           response.raise_for_status()
           return response

Logging
-------

-  Use the context.logger for logging within tasks.
-  Log relevant information at the start and end of tasks, and when
   catching exceptions.

.. code:: python

   context.logger.info("running SimpleBatchReaderTask")
   context.logger.error(f"API request failed: {e}")

Transformer Functions
---------------------

Column functions
^^^^^^^^^^^^^^^^

Here’s an example of a column function that returns ``child`` when the
age is less than 13, ``teenager`` when the age is between 13 and 19, and
``adult`` when the age is above 19.

.. code:: python

   def life_stage(col):
       return (
           F.when(col < 13, "child")
           .when(col.between(13, 19), "teenager")
           .when(col > 19, "adult")
       )

The ``life_stage()`` function will return ``null`` when ``col`` is
``null``. All built-in Spark functions gracefully handle the ``null``
case, so we don’t need to write explicit ``null`` logic in the
``life_stage()`` function.

Column functions can also be optimized by the Spark compiler, so this is
a good way to write code.

Schema Dependent Custom DataFrame Transformations
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

Custom DataFrame transformations are functions that take a DataFrame as
an argument and return a DataFrame. Custom DataFrame transformations are
easy to test and reuse, so they’re a good way to structure Spark code.

Let’s wrap the ``life_stage`` column function that we previously defined
in a schema dependent custom transformation.

.. code:: python

   def with_life_stage(df):
       return df.withColumn("life_stage", life_stage(F.col("age")))

You can invoke this schema dependent custom DataFrame transformation
with the ``transform`` method:

.. code:: python

   df.transform(with_life_stage)

``with_life_stage`` is an example of a schema dependent custom DataFrame
transformation because it must be run on DataFrame that contains an
``age`` column. Schema dependent custom DataFrame transformations make
assumptions about the schema of the DataFrames they’re run on.

Schema Independent Custom DataFrame Transformations
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

Let’s refactor the ``with_life_stage`` function so that it takes the
column name as a parameter and does not depend on the underlying
DataFrame schema (this syntax works as of PySpark 3.3.0).

.. code:: python

   def with_life_stage2(df, col_name):
       return df.withColumn("life_stage", life_stage(F.col(col_name)))

There are two ways to invoke this schema independent custom DataFrame
transformation:

.. code:: python

   # invoke with a positional argument
   df.transform(with_life_stage2, "age")

   # invoke with a keyword argument
   df.transform(with_life_stage2, col_name="age")

What type of DataFrame transformation should be used
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

Schema dependent transformations should be used for functions that rely
on a large number of columns or functions that are only expected to be
run on a certain schema (e.g. a data table with a schema that doesn’t
change).

Schema independent transformations should be run for functions that will
be run on DataFrames with different schemas.

Best practices for ``None`` and ``null``
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

``null`` should be used in DataFrames for values that are `unknown,
missing, or
irrelevant <https://medium.com/@mrpowers/dealing-with-null-in-spark-cfdbb12f231e#.fk27ontik>`__.

Spark core functions frequently return ``null`` and your code can also
add ``null`` to DataFrames (by returning ``None`` or relying on Spark
functions that return ``null``).

Let’s take a look at a ``with_full_name`` custom DataFrame
transformation:

.. code:: python

   def with_full_name(df):
       return df.withColumn(
           "full_name", F.concat_ws(" ", F.col("first_name"), F.col("last_name"))
       )

``with_full_name`` returns ``null`` if either the ``first_name`` or
``last_name`` is ``null``. Let’s take a look at an example:

Let’s create a DataFrame to demonstrate the functionality of
``with_full_name``:

.. code:: python

   df = spark.createDataFrame(
       [("Marilyn", "Monroe"), ("Abraham", None), (None, None)]
   ).toDF("first_name", "last_name")

Here are the DataFrame contents:

::

   +----------+---------+
   |first_name|last_name|
   +----------+---------+
   |   Marilyn|   Monroe|
   |   Abraham|     null|
   |      null|     null|
   +----------+---------+

Here’s how to invoke the custom DataFrame transformation using the
``transform`` method:

.. code:: python

   df.transform(with_full_name).show()

::

   +----------+---------+--------------+
   |first_name|last_name|     full_name|
   +----------+---------+--------------+
   |   Marilyn|   Monroe|Marilyn Monroe|
   |   Abraham|     null|       Abraham|
   |      null|     null|              |
   +----------+---------+--------------+

The ``nullable`` property of a column should be set to ``false`` if the
column should not take ``null`` values. Look at the ``nullable``
properties in the resulting DataFrame.

::

   df.transform(with_full_name).printSchema()

   root
    |-- first_name: string (nullable = true)
    |-- last_name: string (nullable = true)
    |-- full_name: string (nullable = false)

``first_name`` and ``last_name`` have ``nullable`` set to ``true``
because they can take ``null`` values. ``full_name`` has ``nullable``
set to ``false`` because every value must be a non-null string.

See the section on User Defined Functions for more information about
properly handling the ``None`` / ``null`` values for UDFs.

Testing column functions
^^^^^^^^^^^^^^^^^^^^^^^^

You can use the `chispa <https://github.com/MrPowers/chispa>`__ library
to unit test your PySpark column functions and custom DataFrame
transformations.

Let’s look at how to unit test the ``life_stage`` column function:

.. code:: python

   def life_stage(col):
       return (
           F.when(col < 13, "child")
           .when(col.between(13, 19), "teenager")
           .when(col > 19, "adult")
       )

Create a test DataFrame with the expected return value of the function
for each row:

.. code:: python

   df = spark.createDataFrame(
       [
           ("karen", 56, "adult"),
           ("jodie", 16, "teenager"),
           ("jason", 3, "child"),
           (None, None, None),
       ]
   ).toDF("first_name", "age", "expected")

Now invoke the function and create the result DataFrame:

.. code:: python

   res = df.withColumn("actual", life_stage(F.col("age")))

Assert that the actual return value equals the expected value:

.. code:: python

   import chispa

   chispa.assert_column_equality(res, "expected", "actual")

You should always test the ``None`` / ``null`` case to make sure that
your code behaves as expected.

Testing custom DataFrame transformations
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

Suppose you have the following custom transformation:

.. code:: python

   def with_full_name(df):
       return df.withColumn(
           "full_name", F.concat_ws(" ", F.col("first_name"), F.col("last_name"))
       )

Create a minimalistic input DataFrame with some test data:

.. code:: python

   input_df = spark.createDataFrame(
       [("Marilyn", "Monroe"), ("Abraham", None), (None, None)]
   ).toDF("first_name", "last_name")

Create a DataFrame with the expected data:

.. code:: python

   expected_df = spark.createDataFrame(
       [
           ("Marilyn", "Monroe", "Marilyn Monroe"),
           ("Abraham", None, "Abraham"),
           (None, None, ""),
       ]
   ).toDF("first_name", "last_name", "full_name")

Make sure the expected DataFrame equals the input DataFrame with the
custom DataFrame transformation applied:

.. code:: python

   chispa.assert_df_equality(
       expected_df, input_df.transform(with_full_name), ignore_nullable=True
   )

User defined functions
----------------------

You can write User Defined Functions (UDFs) when you need to write code
that leverages Python programming features / Python libraries that
aren’t accessible in Spark.

Here’s an example of a UDF that appends “is fun” to a string:

.. code:: python

   from pyspark.sql.types import StringType
   from pyspark.sql.functions import udf


   @udf(returnType=StringType())
   def bad_funify(s):
       return s + " is fun!"

The ``bad_funify`` function is poorly structured because it errors out
when run on a column with ``null`` values.

Here’s how to refactor the UDF to handle ``null`` input without erroring
out:

.. code:: python


   @udf(returnType=StringType())
   def good_funify(s):
       return None if s == None else s + " is fun!"

In this case, a UDF isn’t even necessary. You can just define a regular
column function to get the same functionality:

.. code:: python


   def best_funify(col):
       return F.concat(col, F.lit(" is fun!"))

UDFs `are a black
box <https://jaceklaskowski.gitbooks.io/mastering-spark-sql/spark-sql-udfs-blackbox.html>`__
from the Spark compiler’s perspective and should be avoided whenever
possible.

Function naming conventions
---------------------------

-  ``with`` precedes transformations that add columns:

   -  ``with_cool_cat()`` adds the column ``cool_cat`` to a DataFrame

   -  ``with_is_nice_person()`` adds the column ``is_nice_person`` to a
      DataFrame.

-  ``filter`` precedes transformations that remove rows:

   -  ``filter_negative_growth_rate()`` removes the data rows where the
      ``growth_rate`` column is negative

   -  ``filter_invalid_zip_codes()`` removes the data with a malformed
      ``zip_code``

-  ``enrich`` precedes transformations that clobber columns. Clobbing
   columns should be avoided when possible, so ``enrich``
   transformations should only be used in rare circumstances.

-  ``explode`` precedes transformations that add rows to a DataFrame by
   “exploding” a row into multiple rows.
