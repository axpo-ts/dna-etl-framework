=====
CI/CD
=====

CI
--

``.github/workflows/ci.yml``

Triggers
~~~~~~~~

-  When pushing on **main** or **develop** branches:

   -  Pushing changes on a branch called ``my-new-branch`` would trigger
      the workflow
   -  Pushing changes on a branch called ``feature/a-new-feature`` would
      **not** trigger the workflow

-  Pull requests on ``main`` or ``develop``

Checks
~~~~~~

-  Unit test: ``unit_test``
-  Lint: ``lint_check`` and ``format_check``
-  Validate: ``databricks bundle validate``
-  Copy & Paste Detection:
   ``pmd cpd --language python --minimum-tokens 50 --dir .``
-  Integration tests (databricks): publishes results as
   ``unit-testresults.xml``

CD
--

``.github/workflows/cd.yml``

.. _triggers-1:

Triggers
~~~~~~~~

-  On workflow call
-  Required parameter: ``target_environment:string``

Initialisation
~~~~~~~~~~~~~~

-  Target environments: ``lower`` and ``upper``:

   -  Converts the ``target_environment`` parameter to all **lowercase**
      and all **uppercase** strings
   -  These two versions of the string are used in deployment:

      -  ``target_environment_lower``
      -  ``target_environment_upper``

Deployment
~~~~~~~~~~

-  Credentials: ``creds`` are composed using
   **target_environment_upper** to retrieve Azure client id, client
   secret and tenant id for that environment from github secrets

-  ``databricks bundle deploy`` on the environment with
   **target_environment_lower**

-  ``databricks bundle run`` on the environment with
   **target_environment_lower** for non-dev environments to trigger
   migrations workflow

Workflow Jobs
~~~~~~~~~~~~~

-  ``unit_test``: Runs unit tests using PDM
-  ``lint``: Runs linting and format checks using PDM
-  ``validate``: Validates Databricks bundle using Azure login and
   Databricks CLI
-  ``cpd``: Copy and Paste Detection using the `PMD Static Code
   Analyzer <https://pmd.github.io/>`__. Will fail a build if copy/paste
   block(s) detected. To allow build to progress regardless of findings
   update the CPD step in ci.yml to have “continue-on-error” to true.
-  ``integration_tests``: Runs integration tests and publishes results
-  ``deploy-dev``: Deploys to the ``dev`` environment
-  ``deploy-test``: Deploys to the ``test`` environment after ``dev``
   deployment
-  ``deploy-prod``: Deploys to the ``prod`` environment on ``main``
   branch

--------------

`Home <../README.md>`__
