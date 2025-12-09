===============================
PEP 8 Python Coding Style Guide
===============================

General Guidelines

Follow PEP 8 for Python code style.
-----------------------------------

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

Indentation
-----------

Use 4 spaces per indentation level.

.. code:: python

   def example_function():
       if True:
           print("Follow PEP 8")

Line Length
-----------

Limit all lines to a maximum of 79 characters. For long import
statements or complex expressions, use parentheses for line breaks.

.. code:: python

   # Correct
   import os
   import sys
   from collections import namedtuple, deque

   # Correct
   def long_function_name(
           var_one, var_two, var_three,
           var_four):
       print(var_one)

Blank Lines
-----------

Surround top-level function and class definitions with two blank lines.
Use one blank line to separate method definitions inside a class.

.. code:: python

   class MyClass:
       def method_one(self):
           pass

       def method_two(self):
           pass

Comments
--------

Use comments to explain code that may not be immediately clear. Use #
for inline comments and block comments.

.. code:: python

   # This is a block comment
   x = 5  # This is an inline comment

Docstrings
----------

Use triple double quotes for docstrings. Start with a one-line summary,
followed by a more detailed description if necessary.

.. code:: python

   def example_function(param1, param2):
       """
       This is an example function.

       Parameters:
       param1: Description of param1
       param2: Description of param2

       Returns:
       Description of return value
       """
       return param1 + param2

Naming Conventions
------------------

-  Use snake_case for function and variable names.
-  Use CamelCase for class names.
-  Use UPPER_CASE for constants.

.. code:: python

   class MyClass:
       def my_method(self):
           my_variable = 10

Error Handling
--------------

-  Use exceptions to handle errors.
-  Provide meaningful error messages.

.. code:: python

   try:
       x = 1 / 0
   except ZeroDivisionError as e:
       print(f"Error: {e}")

Example Code from Repository Example 1: pytest_databricks.py

.. code:: python

   import os
   import sys
   import pytest

   # Get the path to the directory for this file in the workspace.
   dir_root = os.path.dirname(os.path.realpath(__file__))
   # Switch to the root directory.
   os.chdir(dir_root)

   ## Skip writing .pyc files to the bytecode cache on the cluster.
   sys.dont_write_bytecode = True

   ## Now run pytest from the root directory
   print(dir_root)
   retcode = pytest.main(sys.argv[1:])

Example 2: task_runner_notebook.py

.. code:: python

   import json
   import logging
   import time
   from collections.abc import Callable
   from typing import ClassVar

   from data_platform.common import Task
   from data_platform.tasks.reader_tasks import (
       SimpleBatchReaderTask, SimpleBatchFileReaderTask
   )

   TASKS = [
       SimpleBatchReaderTask,
       SimpleBatchFileReaderTask,
   ]

   class TaskRunner(Task):
       """Task Runner for executing a sequence of tasks."""

       def launch(self) -> None:
           """Launch the Task Runner."""
           self.logger.info("Starting Task Runner")
           start_time = time.time()
           tasks_conf = Configuration(self.conf["tasks"])
           self.logger.info(tasks_conf.as_dict())
           tasks = build_tasks(tasks_conf)
           context = TaskContext(configuration=tasks_conf, spark=self.spark, logger=self._prepare_logger())
           self.logger.info(f"Start time: {time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(start_time))}")
           execute(tasks=tasks, task_context=context)
           end_time = time.time()
           self.logger.info(f"End time: {time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(end_time))}")
           execution_time = end_time - start_time
           self.logger.info(f"Execution time: {execution_time} seconds")
