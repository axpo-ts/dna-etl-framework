from pyspark.sql.types import (
    StringType,
)

from data_platform.data_model.metadata_common import standard_tags
from data_platform.data_model.metadata_common.common_columns import StandardColumn

ENTSOE_COMMON_TAGS = {
    standard_tags.ConfidentialityLevel.KEY: standard_tags.ConfidentialityLevel.C2_INTERNAL,
    standard_tags.DataDomain.KEY: standard_tags.DataDomain.FUNDAMENTAL,
    standard_tags.DataOwner.KEY: standard_tags.DataOwner.REMI_JANNER,
    standard_tags.LicensedData.KEY: standard_tags.LicensedData.FALSE,
    standard_tags.PIIClassification.KEY: standard_tags.PIIClassification.NO_PII,
}

# ============================================================================
# AREA/LOCATION COLUMNS
# ============================================================================

AreaCodeColumn = StandardColumn(
    name="area_code",
    comment="A unique alphanumeric identifier for a specific geographic area, market zone, or control area relevant to the data record",
    data_type=StringType(),
    nullable=True,
)

AreaTypeCodeColumn = StandardColumn(
    name="area_type_code",
    comment="The type of the area. Typical values include: BZN - Bidding Zone, CTA - Control Area, CTY - Country",
    data_type=StringType(),
    nullable=True,
)

AreaNameColumn = StandardColumn(
    name="area_name",
    comment="The name of the geographic area, market zone, or control area relevant to the data record",
    data_type=StringType(),
    nullable=True,
)

AreaMapCodeColumn = StandardColumn(
    name="area_map_code",
    comment="A standardized code that uniquely identifies a market area, bidding zone, country, or TSO control area within the ENTSO-E data model",
    data_type=StringType(),
    nullable=True,
)



InAreaMapCodeColumn = StandardColumn(
    name="in_area_map_code",
    comment="The map code where power is flowing in",
    data_type=StringType(),
    nullable=True,
)


OutAreaMapCodeColumn = StandardColumn(
    name="out_area_map_code",
    comment="The map code where power is flowing out",
    data_type=StringType(),
    nullable=True,
)



class EntsoeStandardColumns:
    """Convenience accessor for all standard columns.

    Allows access via standard_columns.DeliveryStartColumn.to_struct_field()
    """

    AreaCodeColumn = AreaCodeColumn
    AreaTypeCodeColumn = AreaTypeCodeColumn
    AreaNameColumn = AreaNameColumn
    AreaMapCodeColumn = AreaMapCodeColumn
    InAreaMapCodeColumn = InAreaMapCodeColumn
    OutAreaMapCodeColumn = OutAreaMapCodeColumn


# Singleton instance for convenient access
entsoe_standard_columns = EntsoeStandardColumns()
