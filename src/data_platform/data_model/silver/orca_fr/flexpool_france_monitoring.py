from __future__ import annotations

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
