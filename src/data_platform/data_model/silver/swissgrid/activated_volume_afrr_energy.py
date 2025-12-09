from __future__ import annotations

from pyspark.sql.types import FloatType, StructField, StructType, TimestampType

from data_platform.data_model import StaticTableModel, TableIdentifier
from data_platform.data_model.metadata_common import standard_columns, standard_tags

from .swissgrid_common import SWISSGRID_COMMON_TAGS, SWISSGRID_LICENSE

activated_volume_afrr_energy_table = StaticTableModel(
    identifier=TableIdentifier(catalog="silver", schema="swissgrid", name="activated_volume_afrr_energy"),
    schema=StructType([
        StructField("time_stamp_utc", TimestampType(), False, {"comment": "The datetime of the start of the delivery period in UTC"}),
        StructField("volume", FloatType(), True, {"comment": "Secondary energy demand"}),
        standard_columns.UnitColumn.to_struct_field(),
        standard_columns.LicenseColumn.to_struct_field(),
        standard_columns.DataSourceColumn.to_struct_field(),
        standard_columns.DataSystemColumn.to_struct_field(),
    ]),
    comment="Activation signal of Swissgrid in the Swiss aFRR (PICASSO) market.",
    sources=[
       TableIdentifier(catalog="bronze", schema="swissgrid", name="activation_signal"),
       TableIdentifier(catalog="bronze", schema="swissgrid", name="afrr_activations"),
    ],
    license=SWISSGRID_LICENSE,
    tags={
        **SWISSGRID_COMMON_TAGS,
        standard_tags.DataOwner.KEY: standard_tags.DataOwner.DAVID_PERRAUDIN,
        standard_tags.LicensedData.KEY: standard_tags.LicensedData.TRUE,
    },
    primary_keys=["time_stamp_utc"],
)
