from data_platform.data_model.metadata_common import standard_tags

METEOLOGICA_COMMON_TAGS = {
    standard_tags.ConfidentialityLevel.KEY: standard_tags.ConfidentialityLevel.C2_INTERNAL,
    standard_tags.DataDomain.KEY: standard_tags.DataDomain.FUNDAMENTAL,
    standard_tags.LicensedData.KEY: standard_tags.LicensedData.TRUE,
    standard_tags.PIIClassification.KEY: standard_tags.PIIClassification.NO_PII,
    standard_tags.DataOwner.KEY: standard_tags.DataOwner.IRIO_CALASSO,
}
