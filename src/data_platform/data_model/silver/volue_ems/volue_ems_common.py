"""Common definitions for Volue EMS silver tables."""

from __future__ import annotations

from dataclasses import dataclass

from pyspark.sql.types import StringType

from data_platform.data_model.metadata_common import standard_tags
from data_platform.data_model.metadata_common.common_columns import StandardColumn

# Common license for all Volue EMS tables
VOLUE_EMS_LICENSE = "DNA_ALL_ACCESS"

# Common tags shared across all Volue EMS tables
VOLUE_EMS_COMMON_TAGS = {
    standard_tags.ConfidentialityLevel.KEY: standard_tags.ConfidentialityLevel.C2_INTERNAL,
    standard_tags.PIIClassification.KEY: standard_tags.PIIClassification.NO_PII,
    standard_tags.DataOwner.KEY: standard_tags.DataOwner.JOHANNA_HEIDI_KAUPPINEN,
    standard_tags.LicensedData.KEY: standard_tags.LicensedData.FALSE,
}

# Volue EMS-specific standard columns
NameColumn = StandardColumn(
    name="name",
    data_type=StringType(),
    nullable=False,
    comment="Full name"
)

AxpoCompanyCodeColumn = StandardColumn(
    name="axpo_company_code",
    data_type=StringType(),
    nullable=False,
    comment="Billing Axpo Entity (SE or NO)"
)

CustomerObjectColumn = StandardColumn(
    name="customer_object",
    data_type=StringType(),
    nullable=False,
    comment="Customer code"
)

InvoicingComponentColumn = StandardColumn(
    name="invoicing_component",
    data_type=StringType(),
    nullable=False,
    comment="Type of component included in the invoice"
)

TimeLevelColumn = StandardColumn(
    name="time_level",
    data_type=StringType(),
    nullable=False,
    comment="Time level (currently Hourly)"
)


@dataclass
class VolueEmsStandardColumns:
    """Accessor class for Volue EMS-specific standard columns."""

    NameColumn: StandardColumn = NameColumn
    AxpoCompanyCodeColumn: StandardColumn = AxpoCompanyCodeColumn
    CustomerObjectColumn: StandardColumn = CustomerObjectColumn
    InvoicingComponentColumn: StandardColumn = InvoicingComponentColumn
    TimeLevelColumn: StandardColumn = TimeLevelColumn


volue_ems_standard_columns = VolueEmsStandardColumns()
