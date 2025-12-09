from __future__ import annotations

from pyspark.sql.types import (
    StructType,
)

from data_platform.data_model import StaticTableModel, TableIdentifier
from data_platform.data_model.metadata_common import standard_columns, standard_tags
from data_platform.data_model.silver.nordpool.nordpool_common import (
    NORDPOOL_COMMON_TAGS,
    NORDPOOL_LICENSE,
    nordpool_standard_columns,
)

nordpool_flow_table = StaticTableModel(
    identifier=TableIdentifier(
        catalog="silver",
        schema="nordpool",
        name="nordpool_flow",
    ),
    schema=StructType(
        [
            standard_columns.DeliveryStartColumn.to_struct_field(),
            standard_columns.DeliveryEndColumn.to_struct_field(),
            standard_columns.DurationColumn.to_struct_field(),
            nordpool_standard_columns.FromAreaColumn.to_struct_field(),
            nordpool_standard_columns.ToAreaColumn.to_struct_field(),
            nordpool_standard_columns.ConnectionColumn.to_struct_field(),
            nordpool_standard_columns.ImportColumn.to_struct_field(),
            nordpool_standard_columns.ExportColumn.to_struct_field(),
            nordpool_standard_columns.NetPositionColumn.to_struct_field(),
            standard_columns.UpdatedAtSourceColumn.to_struct_field(),
            standard_columns.CommodityColumn.to_struct_field(),
            standard_columns.LicenseColumn.to_struct_field(),
            standard_columns.DataSourceColumn.to_struct_field(),
            standard_columns.DataSystemColumn.to_struct_field(),
        ]
    ),
    comment=(
        "This table contains the auction flows per area for the DayAhead market. "
        "Describing how much power are exchanged between areas. The flow is coming from Euphemia, "
        "up until 30th of October 2024, where the Scheduled Physical Flow data takes over. "
        "Flows are always described from an area point of view. Some flows will have a loss on the cable - "
        "imports in an area is always after losses, and exports are before losses."
    ),
    sources=[
        TableIdentifier(catalog="bronze", schema="nordpool", name="flow"),
    ],
    license=NORDPOOL_LICENSE,
    tags={
        **NORDPOOL_COMMON_TAGS,
        standard_tags.DataDomain.KEY: standard_tags.DataDomain.FUNDAMENTAL,
        standard_tags.DataSubdomain.KEY: standard_tags.DataSubdomain.TRANSMISSION,
    },
    primary_keys=("delivery_start", "deliver_end", "from_area", "to_area"),
    partition_cols=(),
    liquid_cluster_cols=(),
)
