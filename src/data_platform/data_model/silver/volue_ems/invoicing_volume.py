from __future__ import annotations

from pyspark.sql.types import DoubleType, StructField, StructType, TimestampType

from data_platform.data_model import StaticTableModel, TableIdentifier
from data_platform.data_model.metadata_common import standard_columns, standard_tags

from .volue_ems_common import VOLUE_EMS_COMMON_TAGS, VOLUE_EMS_LICENSE, volue_ems_standard_columns

invoicing_volume_table = StaticTableModel(
    identifier=TableIdentifier(
        catalog="silver",
        schema="volue_ems",
        name="invoicing_volume"
    ),
    schema=StructType([
        volue_ems_standard_columns.NameColumn.to_struct_field(),
        volue_ems_standard_columns.AxpoCompanyCodeColumn.to_struct_field(),
        volue_ems_standard_columns.CustomerObjectColumn.to_struct_field(),
        volue_ems_standard_columns.InvoicingComponentColumn.to_struct_field(
            comment="Type of volume included in the invoice"
        ),
        standard_columns.UnitColumn.to_struct_field(comment="Volume unit"),
        volue_ems_standard_columns.TimeLevelColumn.to_struct_field(),
        StructField("timestamp", TimestampType(), False, {"comment": "Timestamp"}),
        StructField("value", DoubleType(), False, {"comment": "Volume at timestamp"}),
        standard_columns.LicenseColumn.to_struct_field(),
    ]),
    comment="Invoicing volumes from CIS_ENER data group",
    sources=[
        TableIdentifier(catalog="bronze", schema="volue_ems", name="nordic_physical"),
    ],
    license=VOLUE_EMS_LICENSE,
    tags={
        **VOLUE_EMS_COMMON_TAGS,
        standard_tags.DataDomain.KEY: standard_tags.DataDomain.METERED_AND_VOLUME,
    },
    primary_keys=("name", "timestamp"),
)
