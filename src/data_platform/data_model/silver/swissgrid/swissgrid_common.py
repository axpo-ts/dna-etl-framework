"""Common definitions for Swissgrid silver tables."""

from __future__ import annotations

from data_platform.data_model.metadata_common import standard_tags

# Common license for all Swissgrid tables
SWISSGRID_LICENSE = "DNA_SWISSGRID"

# Common tags shared across all Swissgrid tables
SWISSGRID_COMMON_TAGS = {
    standard_tags.ConfidentialityLevel.KEY: standard_tags.ConfidentialityLevel.C2_INTERNAL,
    standard_tags.PIIClassification.KEY: standard_tags.PIIClassification.NO_PII,
    standard_tags.DataDomain.KEY: standard_tags.DataDomain.MARKET,
    standard_tags.DataSubdomain.KEY: standard_tags.DataSubdomain.BALANCING_AND_RESERVE,
}
