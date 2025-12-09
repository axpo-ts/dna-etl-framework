"""Common column definitions and tags for Nordpool silver tables."""

from __future__ import annotations

from pyspark.sql.types import DoubleType, StringType

from data_platform.data_model.metadata_common import standard_tags
from data_platform.data_model.metadata_common.common_columns import StandardColumn

# License identifier for all Nordpool data
NORDPOOL_LICENSE = "NORDPOOL_STD_NORDPOOL"

# Common tags for all Nordpool tables
NORDPOOL_COMMON_TAGS = {
    standard_tags.ConfidentialityLevel.KEY: standard_tags.ConfidentialityLevel.C2_INTERNAL,
    standard_tags.PIIClassification.KEY: standard_tags.PIIClassification.NO_PII,
    standard_tags.DataOwner.KEY: standard_tags.DataOwner.KNUT_STENSROD,
    standard_tags.LicensedData.KEY: standard_tags.LicensedData.TRUE,
}

# ============================================================================
# NORDPOOL-SPECIFIC COLUMNS (not in standard_columns)
# ============================================================================

FromAreaColumn = StandardColumn(
    name="from_area",
    comment="The area from which the flow is imported, e.g. SE1, FI, DK1, etc.",
    data_type=StringType(),
    nullable=True,
)

ToAreaColumn = StandardColumn(
    name="to_area",
    comment="The area to which the flow is exported, e.g. SE1, FI, DK1, etc.",
    data_type=StringType(),
    nullable=True,
)

ConnectionColumn = StandardColumn(
    name="connection",
    comment="The connection between two areas, e.g. SE1-FI, DK1-DK2, etc.",
    data_type=StringType(),
    nullable=True,
)

ImportColumn = StandardColumn(
    name="import",
    comment="The amount of flow imported from the from_area to the to_area in the specified unit.",
    data_type=DoubleType(),
    nullable=True,
)

ExportColumn = StandardColumn(
    name="export",
    comment="The amount of flow exported from the to_area to the from_area in the specified unit.",
    data_type=DoubleType(),
    nullable=True,
)

NetPositionColumn = StandardColumn(
    name="net_position",
    comment="The net position of the flow, calculated as import - export.",
    data_type=DoubleType(),
    nullable=True,
)

# Standard column set for Nordpool tables with delivery periods
# Using standard_columns where available

class NordpoolStandardColumns:
    """Convenience accessor for all standard columns.

    Allows access via standard_columns.DeliveryStartColumn.to_struct_field()
    """

    FromAreaColumn = FromAreaColumn
    ToAreaColumn = ToAreaColumn
    ConnectionColumn = ConnectionColumn
    ImportColumn = ImportColumn
    ExportColumn = ExportColumn
    NetPositionColumn = NetPositionColumn


# Singleton instance for convenient access
nordpool_standard_columns = NordpoolStandardColumns()
