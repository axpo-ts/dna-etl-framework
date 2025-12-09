# Databrick notebook source
"""Common columns and tags for NENA schema tables."""

from __future__ import annotations

from pyspark.sql.types import DoubleType, StringType, TimestampType

from data_platform.data_model.metadata_common import standard_tags
from data_platform.data_model.metadata_common.common_columns import StandardColumn

# Common tags used across all NENA tables
NENA_COMMON_TAGS = {
    standard_tags.ConfidentialityLevel.KEY: standard_tags.ConfidentialityLevel.C2_INTERNAL,
    standard_tags.LicensedData.KEY: standard_tags.LicensedData.TRUE,
    standard_tags.PIIClassification.KEY: standard_tags.PIIClassification.NO_PII,
    standard_tags.DataOwner.KEY: standard_tags.DataOwner.KNUT_STENSROD,
}

# NENA-specific columns (used in all 7 tables)
NameColumn = StandardColumn(name="name", data_type=StringType(), nullable=False, comment="Curve name")
AreaColumn = StandardColumn(name="area", data_type=StringType(), nullable=False, comment="Area code (e.g., NO1, SE3, etc.). Might be empty if no area is defined for the curve type.")
ModelColumn = StandardColumn(name="model", data_type=StringType(), nullable=False, comment="Model forecast name. The model depends on the curve type and are defined by Nena. Might be empty if no model is defined for the curve type.")
ScenarioColumn = StandardColumn(name="scenario", data_type=StringType(), nullable=False, comment="Weather scenario name. The scenario depends on the curve type and are defined by Nena. Might be empty if no scenario is defined for the curve type.")
ResolutionColumn = StandardColumn(name="resolution", data_type=StringType(), nullable=False, comment="Time resolution (e.g. Hourly, Daily, etc.)")
TimestampColumn = StandardColumn(name="timestamp", data_type=TimestampType(), nullable=False, comment="Timestamp")
ValueColumn = StandardColumn(name="value", data_type=DoubleType(), nullable=False, comment="Value at timestamp")
DescriptionColumn = StandardColumn(name="description", data_type=StringType(), nullable=False, comment="Curve description")
