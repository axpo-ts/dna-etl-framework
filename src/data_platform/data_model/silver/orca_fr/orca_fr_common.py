"""Common column definitions and tags for ORCA FR silver tables."""

from __future__ import annotations

from data_platform.data_model.metadata_common import standard_tags

# License identifier for ORCA FR data
ORCA_FR_LICENSE = "DNA_ORCA_FR"

# Common tags for ORCA FR tables
ORCA_FR_COMMON_TAGS = {
    standard_tags.ConfidentialityLevel.KEY: standard_tags.ConfidentialityLevel.C2_INTERNAL,
    standard_tags.PIIClassification.KEY: standard_tags.PIIClassification.NO_PII,
    standard_tags.LicensedData.KEY: standard_tags.LicensedData.TRUE,
}
