"""Common column definitions and tags for Neuron schema tables.

This module provides shared column definitions and tag configurations
for all tables in the silver.neuron schema.
"""

from __future__ import annotations

from pyspark.sql.types import DecimalType, DoubleType, StringType, TimestampType

from data_platform.data_model.metadata_common.common_columns import StandardColumn
from data_platform.data_model.metadata_common.tags_enum import (
    ConfidentialityLevel,
    DataOwner,
    LicensedData,
    PIIClassification,
)

# Common tags (shared across all neuron tables)
NEURON_COMMON_TAGS = {
    ConfidentialityLevel.KEY: ConfidentialityLevel.C2_INTERNAL,
    PIIClassification.KEY: PIIClassification.NO_PII,
    DataOwner.KEY: DataOwner.IGNACIO_ORTIZ_DE_ZUNIGA,
    LicensedData.KEY: LicensedData.TRUE,
}

# Columns used in 2+ tables
DateFormattedColumn = StandardColumn(name="date_formatted", data_type=TimestampType(), nullable=True, comment="Timestamp of the record, formatted with hours and minutes.")
DateColumn = StandardColumn(name="date", data_type=TimestampType(), nullable=True, comment="Date of the record.")
Cups20Column = StandardColumn(name="cups20", data_type=StringType(), nullable=True, comment="CUPS is a unique identifier for electricity supply points.")
Cups22Column = StandardColumn(name="cups22", data_type=StringType(), nullable=True, comment="Combination of CUPS plus line code.")
TypeColumn = StandardColumn(name="type", data_type=StringType(), nullable=True, comment="Main classification of the measurement.")
SubtypeColumn = StandardColumn(name="subtype", data_type=StringType(), nullable=True, comment="Detailed classification within the main type.")
FrequencyColumn = StandardColumn(name="frequency", data_type=StringType(), nullable=True, comment="Measurement frequency or interval (e.g., HOUR, Q-HOUR).")
HourColumn = StandardColumn(name="hour", data_type=DecimalType(38, 0), nullable=True, comment="Hour/Quarter component of the timestamp.")
HOrderColumn = StandardColumn(name="h_order", data_type=DecimalType(38, 0), nullable=True, comment="Hour/Quarter order or sequence within the day.")
ValueColumn = StandardColumn(name="value", data_type=DoubleType(), nullable=True, comment="Measured or calculated value.")
TimestampCreationColumn = StandardColumn(name="timestamp_creation", data_type=TimestampType(), nullable=True, comment="Timestamp when the record was originally created in the source system.")
TimestampUpdateColumn = StandardColumn(name="timestamp_update", data_type=TimestampType(), nullable=True, comment="Timestamp when the record was last updated in the source system.")
SrcCreatedByColumn = StandardColumn(name="src_created_by", data_type=StringType(), nullable=True, comment="User or system that created the record in the source system.")
SrcCreatedAtColumn = StandardColumn(name="src_created_at", data_type=TimestampType(), nullable=True, comment="Timestamp when the record was created in the source system.")
