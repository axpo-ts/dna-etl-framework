"""Common column definitions and tags for Reference silver tables."""

from __future__ import annotations

from data_platform.data_model.metadata_common import standard_tags

# License identifier for Reference data
REFERENCE_LICENSE = "DNA-ALL-ACCESS"

# Common tags for Reference tables
REFERENCE_COMMON_TAGS = {
    standard_tags.ConfidentialityLevel.KEY: standard_tags.ConfidentialityLevel.C2_INTERNAL,
    standard_tags.PIIClassification.KEY: standard_tags.PIIClassification.NO_PII,
    standard_tags.LicensedData.KEY: standard_tags.LicensedData.FALSE,
}
