"""Common definitions for Bridge Tool silver tables."""

from __future__ import annotations

from data_platform.data_model.metadata_common import standard_tags

# Common license for all Bridge Tool tables
# BRIDGETOOL_LICENSE = ""

# Common tags shared across all BRIDGETOOL tables
BRIDGETOOL_COMMON_TAGS = {
    standard_tags.ConfidentialityLevel.KEY: standard_tags.ConfidentialityLevel.C3_CONFIDENTIAL,
    standard_tags.DataDomain.KEY : standard_tags.DataDomain.VALUATION_AND_RISK,
    standard_tags.DataSubdomain.KEY : standard_tags.DataSubdomain.CONTROLLING,
    standard_tags.LicensedData.KEY : standard_tags.LicensedData.TRUE,
    standard_tags.PIIClassification.KEY: standard_tags.PIIClassification.NO_PII,
    standard_tags.DataOwner.KEY: standard_tags.DataOwner.ZUMSTEG_PATRICK,
}
