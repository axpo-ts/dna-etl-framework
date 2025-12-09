"""Common definitions for Volue silver tables."""

from __future__ import annotations

from dataclasses import dataclass

from pyspark.sql.types import ArrayType, IntegerType, StringType, TimestampType

from data_platform.data_model.metadata_common import standard_tags, standard_columns
from data_platform.data_model.metadata_common.common_columns import StandardColumn

# Common license for all Volue tables
VOLUE_LICENSE = "WATTSIGH_INFSERVICE_WATTSIGH"

# License for attribute table (different from other Volue tables)
VOLUE_ATTRIBUTE_LICENSE = "DNA_ALL_ACCESS"

# Common tags shared across all Volue tables (except attribute)
VOLUE_COMMON_TAGS = {
    standard_tags.ConfidentialityLevel.KEY: standard_tags.ConfidentialityLevel.C2_INTERNAL,
    standard_tags.PIIClassification.KEY: standard_tags.PIIClassification.NO_PII,
    standard_tags.DataOwner.KEY: standard_tags.DataOwner.KNUT_STENSROD,
    standard_tags.LicensedData.KEY: standard_tags.LicensedData.TRUE,
}

# Common tags for attribute table (different owner and licensed data)
VOLUE_ATTRIBUTE_COMMON_TAGS = {
    standard_tags.ConfidentialityLevel.KEY: standard_tags.ConfidentialityLevel.C2_INTERNAL,
    standard_tags.PIIClassification.KEY: standard_tags.PIIClassification.NO_PII,
    standard_tags.DataOwner.KEY: standard_tags.DataOwner.YUFAN_HE,
    standard_tags.LicensedData.KEY: standard_tags.LicensedData.FALSE,
}

# Volue-specific standard columns
CurveIdColumn = StandardColumn(
    name="curve_id",
    data_type=IntegerType(),
    nullable=True,
    comment="A unique identifier of the curve as provided by Volue"
)

CurveNameColumn = StandardColumn(
    name="curve_name",
    data_type=StringType(),
    nullable=False,
    comment="The name of the curve including information of category, area, curve_type, unit, time zone, time frequency, data type"
)

DataTypeColumn = StandardColumn(
    name="data_type",
    data_type=StringType(),
    nullable=True,
    comment="The data type associated with the curve (f is forecast, a is actual, n is normal, af is forecast filled with actuals when available)"
)

AreaColumn = StandardColumn(
    name="area",
    data_type=StringType(),
    nullable=True,
    comment="The geographical or market area to which the data applies"
)

CategoriesColumn = StandardColumn(
    name="categories",
    data_type=ArrayType(StringType(), True),
    nullable=True,
    comment="A list of categories associated with the curve (e.g. tt as temperature, riv as river)"
)

TagColumn = StandardColumn(
    name="tag",
    data_type=StringType(),
    nullable=True,
    comment="Each unique Tag value (e.g. 00, Avg, High, 1986) identifies a separate slice such as an ensemble-member, weather-year, or scenario"
)

VolueDescriptionColumn = StandardColumn(
    name="volue_description",
    data_type=StringType(),
    nullable=True,
    comment="A brief description of the curve and data source as provided by Volue"
)

ReferenceDateColumn = StandardColumn(
    name="reference_date",
    data_type=TimestampType(),
    nullable=True,
    comment="The date and time when the forecast was originally published"
)


@dataclass
class VolueStandardColumns:
    """Accessor class for Volue-specific standard columns."""

    CurveIdColumn: StandardColumn = CurveIdColumn
    CurveNameColumn: StandardColumn = CurveNameColumn
    DataTypeColumn: StandardColumn = DataTypeColumn
    AreaColumn: StandardColumn = AreaColumn
    CategoriesColumn: StandardColumn = CategoriesColumn
    TagColumn: StandardColumn = TagColumn
    VolueDescriptionColumn: StandardColumn = VolueDescriptionColumn
    ReferenceDateColumn: StandardColumn = ReferenceDateColumn


volue_standard_columns = VolueStandardColumns()

VOLUE_TIMESERIES_PRIMARY_KEYS = (
    volue_standard_columns.CurveNameColumn.name,
    standard_columns.DeliveryStartColumn.name,
)

VOLUE_TIMESERIES_TAGGED_PRIMARY_KEYS = (
    volue_standard_columns.CurveNameColumn.name,
    standard_columns.DeliveryStartColumn.name,
    volue_standard_columns.TagColumn.name
)
