==========================
LPI Permissions Assignment
==========================

LPI Capture Process
-------------------

The workflow first captures License Product Identifiers (LPIs) through
the following steps:

1. Scans the silver and gold catalogs to identify tables that have a
   ‘license’ column
2. For each identified table, the workflow extracts distinct values from
   the ‘license’ column and stores in the ``lpi_table_map``\ table.

Permission Assignment Process
-----------------------------

The workflow then provides access to the relevent tables through the
following steps:

1. Assignment of SELECT permissions based on the captured LPIs.
2. It also grants BROWSE and USE CATALOG permissions to allow users to
   navigate to their entitled tables.

For detailed documentation on the workflow, please refer to the following Mingle page:
--------------------------------------------------------------------------------------

`Mingle Link <https://mingle.axpo.com/display/DAT/DnA+Data+Access>`__
