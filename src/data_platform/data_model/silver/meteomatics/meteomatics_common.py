"""Common definitions for Meteomatics silver layer data models."""

from __future__ import annotations

from pyspark.sql.types import DecimalType, DoubleType, StringType, TimestampType

from data_platform.data_model.metadata_common import standard_tags
from data_platform.data_model.metadata_common.common_columns import StandardColumn

METEOMATICS_COMMON_TAGS = {
    standard_tags.ConfidentialityLevel.KEY: standard_tags.ConfidentialityLevel.C2_INTERNAL,
    standard_tags.DataDomain.KEY: standard_tags.DataDomain.FUNDAMENTAL,
    standard_tags.LicensedData.KEY: standard_tags.LicensedData.TRUE,
    standard_tags.PIIClassification.KEY: standard_tags.PIIClassification.NO_PII,
}

# ============================================================================
# METEOMATICS-SPECIFIC COLUMNS (not in standard_columns)
# ============================================================================
# Note: Use standard_columns.DeliveryStartColumn, DeliveryEndColumn, DurationColumn
# from metadata_common.common_columns for those fields

CurveNameColumn = StandardColumn(
    name="curve_name",
    comment="The name of the weather variable, e.g., 'temperature', 'pressure', 'geopotential_height'",
    data_type=StringType(),
    nullable=False,
)

ValueColumn = StandardColumn(
    name="value",
    comment="The observed value of the weather variable",
    data_type=DoubleType(),
    nullable=True,
)

LatitudeColumn = StandardColumn(
    name="latitude",
    comment="The latitude of the location for which the observation is made",
    data_type=DecimalType(7, 4),
    nullable=False,
)

LongitudeColumn = StandardColumn(
    name="longitude",
    comment="The longitude of the location for which the observation is made",
    data_type=DecimalType(7, 4),
    nullable=False,
)

ModelColumn = StandardColumn(
    name="model",
    comment="The weather model used for the observation, if applicable",
    data_type=StringType(),
    nullable=True,
)

CurveMemberColumn = StandardColumn(
    name="curve_member",
    comment="The ensemble member or model run identifier",
    data_type=StringType(),
    nullable=False,
)

RequestedTimeColumn = StandardColumn(
    name="requested_time",
    comment="The time when the data was requested from the provider",
    data_type=TimestampType(),
    nullable=False,
)

LatestModelLoadColumn = StandardColumn(
    name="latest_model_load",
    comment="The time of the anticipated model run on the Meteomatics servers",
    data_type=TimestampType(),
    nullable=False,
)




class MeteomaticsStandardColumns:
    """Convenience accessor for all Meteomatics-specific standard columns.

    Allows access via meteomatics_standard_columns.CurveNameColumn.to_struct_field()
    or with comment override: meteomatics_standard_columns.ValueColumn.to_struct_field(metadata={"comment": "..."})

    Note: For DeliveryStartColumn, DeliveryEndColumn, and DurationColumn,
    use standard_columns from metadata_common.common_columns
    """

    CurveNameColumn = CurveNameColumn
    ValueColumn = ValueColumn
    LatitudeColumn = LatitudeColumn
    LongitudeColumn = LongitudeColumn
    ModelColumn = ModelColumn
    CurveMemberColumn = CurveMemberColumn
    RequestedTimeColumn = RequestedTimeColumn
    LatestModelLoadColumn = LatestModelLoadColumn


# Singleton instance for convenient access
meteomatics_standard_columns = MeteomaticsStandardColumns()
