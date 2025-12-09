from __future__ import annotations

from pyspark.sql.types import StructType

from data_platform.data_model import StaticTableModel, TableIdentifier
from data_platform.data_model.metadata_common import standard_columns, standard_tags

attribute_table = StaticTableModel(
    identifier=TableIdentifier(catalog="silver", schema="geocat", name="attribute"),
    schema=StructType(
        [
            standard_columns.DataSystemColumn.to_struct_field(),
            standard_columns.DataSourceColumn.to_struct_field(),
            standard_columns.LicenseColumn.to_struct_field(),
        ]
    ),
    comment="Attributes (e.g.,license, unit, commodity, etc.) that are associated with various entities within Geocat.",
    sources=[
        TableIdentifier(catalog="bronze", schema="geocat", name="attribute"),
    ],
    tags={
        standard_tags.ConfidentialityLevel.KEY: standard_tags.ConfidentialityLevel.C2_INTERNAL,
        standard_tags.DataOwner.KEY: standard_tags.DataOwner.YUFAN_HE,
        standard_tags.LicensedData.KEY: standard_tags.LicensedData.FALSE,
        standard_tags.PIIClassification.KEY: standard_tags.PIIClassification.NO_PII,
    },
)
