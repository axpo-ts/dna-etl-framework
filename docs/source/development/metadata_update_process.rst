=======================
Metadata Update Process
=======================

This document describes the process for managing metadata in the platform, including table tags, table comments, column comments and schema information.

Overview
--------

The job update_metadata_workflow_{env} (resources/general/metadata_update) triggers a metadata update for
tables and schemas in the Unity Catalog. This includes: - Table tags -
Table comments - Column comments

1. Define Metadata for a Table
~~~~~~~~~~~~~~~~~~~~

In order to define metadata for a table, an instance of the class StaticTableModel
must be created in src/dna_platform/data_model/{catalog}/{schema}/table_name.py

Remember to leverage standard_columns and standard_tags as in the following example:

Example:

.. code:: python

   from pyspark.sql.types import DoubleType, StringType, StructField, StructType, TimestampType
   from data_platform.data_model import StaticTableModel, TableIdentifier
   from data_platform.data_model.metadata_common import standard_columns, standard_tags
   from data_platform.data_model.silver.orca_fr.orca_fr_common import (
       ORCA_FR_COMMON_TAGS,
       ORCA_FR_LICENSE,
   )

   flexpool_france_monitoring_table = StaticTableModel(
       identifier=TableIdentifier(
           catalog="silver",
           schema="orca_fr",
           name="flexpool_france_monitoring"
       ),
       schema=StructType([
           StructField("curve_name", StringType(), True, {"comment": "The name of the curve"}),
           StructField("signal_id", StringType(), False, {"comment": "A unique identifier for the signal"}),
           StructField("balance_item_id", StringType(), False, {"comment": "A unique identifier for the asset within the flexpool France"}),
           StructField("timestamp", TimestampType(), False, {"comment": "Timestamp of the recorded value"}),
           StructField("value", DoubleType(), True, {"comment": "Value of the variable"}),
           standard_columns.UnitColumn.to_struct_field(),
           standard_columns.LicenseColumn.to_struct_field(),
           standard_columns.DataSystemColumn.to_struct_field(),
           standard_columns.DataSourceColumn.to_struct_field(),
       ]),
       comment="Table containing a selection of sensors deriving from the SCADA system ORCA (Operating Reserve Cloud Access) which is in use for the BSP activitites of Axpo France within the flexpool in France",
       sources=[
          TableIdentifier(catalog="bronze",schema="orca_fr",name="orca_monitoring_device")
       ],
       partition_cols=("signal_id",),
       primary_keys=("signal_id", "balance_item_id", "timestamp"),
       license=ORCA_FR_LICENSE,
       tags={
           **ORCA_FR_COMMON_TAGS,
           standard_tags.DataOwner.KEY: standard_tags.DataOwner.GIANLUCA_MANCINI,
           standard_tags.DataDomain.KEY: standard_tags.DataDomain.FUNDAMENTAL,
       },
   )



Then the table needs to be registered in the corresponding schema's __init__.py file as follows:

.. code :: python

   from data_platform.data_model.silver.orca_fr.attribute import (
       attribute_table,
   )
   from data_platform.data_model.silver.orca_fr.flexpool_france_monitoring import (
       flexpool_france_monitoring_table,
   )

   __all__ = [
       "attribute_table",
       "flexpool_france_monitoring_table",
   ]


When adding a new schema it has to be added to the catalog level  __init__.py file as follows:

.. code :: python

    from data_platform.data_model.silver import orca_fr

    __all__ = [
        ...,
        "orca_fr",
    ]


**Metadata Update Process**

   - Create StaticTableModel and register it in the data_model folder.
   - Trigger the update_metadata_workflow_{env} job manually after a hotfix or wait for the scheduled regular run.
