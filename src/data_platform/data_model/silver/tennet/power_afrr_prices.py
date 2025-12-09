from __future__ import annotations

from pyspark.sql.types import (
    DoubleType,
    StringType,
    StructField,
    StructType,
)

from data_platform.data_model import StaticTableModel, TableIdentifier
from data_platform.data_model.metadata_common import standard_columns, standard_tags

afrr_schema = StructType(
    [
        standard_columns.DeliveryStartColumn.to_struct_field(),
        standard_columns.DeliveryEndColumn.to_struct_field(),
        standard_columns.DurationColumn.to_struct_field(),
        StructField("price_direction", StringType(), False, {"comment": "The direction of the price, either UP or DOWN"}),
        StructField("average_price", DoubleType(), True, {"comment": "The average price during the delivery period in the specified unit"}),
        StructField("marginal_price", DoubleType(), True, {"comment": "The marginal price during the delivery period in the specified unit"}),
        standard_columns.UnitColumn.to_struct_field(),
        standard_columns.DeliveryAreaColumn.to_struct_field(),
        standard_columns.RefCommodityColumn.to_struct_field(),
        standard_columns.LicenseColumn.to_struct_field(),
        standard_columns.DataSourceColumn.to_struct_field(),
        standard_columns.DataSystemColumn.to_struct_field(),
    ]
)

tennet_price_afrr_energy_table = StaticTableModel(
    identifier=TableIdentifier(catalog="silver", schema="tennet", name="price_afrr_energy"),
    schema=afrr_schema,
    comment="Prices for Automatic Frequency Restoration Reserve (aFRR) positive and negative activation in the Netherlands.",
    sources=[TableIdentifier(catalog="bronze", schema="tennet", name="merit_order_list")],
    tags={
        standard_tags.ConfidentialityLevel.KEY: standard_tags.ConfidentialityLevel.C2_INTERNAL,
        standard_tags.DataDomain.KEY: standard_tags.DataDomain.MARKET,
        standard_tags.DataSubdomain.KEY: standard_tags.DataSubdomain.BALANCING_AND_RESERVE,
        standard_tags.LicensedData.KEY: standard_tags.LicensedData.FALSE,
        standard_tags.PIIClassification.KEY: standard_tags.PIIClassification.NO_PII,
        standard_tags.DataOwner.KEY: standard_tags.DataOwner.CHRISTOPHE_CATTRY,
    },
    primary_keys=("delivery_area", "delivery_start", "delivery_end", "price_direction"),
    liquid_cluster_cols=(("delivery_area", "delivery_start"),),
)
