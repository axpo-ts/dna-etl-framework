"""Common definitions for RTE silver tables."""

from __future__ import annotations

from data_platform.data_model.metadata_common import standard_tags

# Common license for all RTE tables
RTE_LICENSE = "DNA-ALL-ACCESS"

# Common tags shared across all RTE tables
RTE_COMMON_TAGS = {
    standard_tags.ConfidentialityLevel.KEY: standard_tags.ConfidentialityLevel.C2_INTERNAL,
    standard_tags.PIIClassification.KEY: standard_tags.PIIClassification.NO_PII,
    standard_tags.DataOwner.KEY: standard_tags.DataOwner.GIANLUCA_MANCINI,
    standard_tags.LicensedData.KEY: standard_tags.LicensedData.FALSE,
    standard_tags.DataDomain.KEY: standard_tags.DataDomain.MARKET,
    standard_tags.DataSubdomain.KEY: standard_tags.DataSubdomain.BALANCING_AND_RESERVE,
}
