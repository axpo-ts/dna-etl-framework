from __future__ import annotations

from pyspark.sql.types import StructType

from data_platform.data_model import StaticTableModel, TableIdentifier
from data_platform.data_model.metadata_common import standard_columns, standard_tags

from .swissgrid_common import SWISSGRID_COMMON_TAGS, SWISSGRID_LICENSE

attribute_table = StaticTableModel(
    identifier=TableIdentifier(catalog="silver", schema="swissgrid", name="attribute"),
    schema=StructType([
        standard_columns.UnitColumn.to_struct_field(),
        standard_columns.LicenseColumn.to_struct_field(),
        standard_columns.DataSourceColumn.to_struct_field(),
        standard_columns.DataSystemColumn.to_struct_field(),
    ]),
    comment="Attributes (e.g.,license, unit, etc.) that are associated with various entities within Swissgrid.",
    sources=[
       TableIdentifier(catalog="bronze", schema="swissgrid", name="attributes")
    ],
    license=SWISSGRID_LICENSE,
    tags={
        **SWISSGRID_COMMON_TAGS,
        standard_tags.DataOwner.KEY: standard_tags.DataOwner.YUFAN_HE,
        standard_tags.LicensedData.KEY: standard_tags.LicensedData.FALSE,
    },
    primary_keys=["license"],
)
