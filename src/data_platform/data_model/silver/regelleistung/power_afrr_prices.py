from __future__ import annotations

from pyspark.sql.types import DoubleType, StringType, StructField, StructType

from data_platform.data_model import StaticTableModel, TableIdentifier
from data_platform.data_model.metadata_common import standard_columns

from .regelleistung_common import REGELLEISTUNG_COMMON_TAGS, REGELLEISTUNG_LICENSE

# Define the common schema for both tables
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

regelleistung_price_afrr_capacity_table = StaticTableModel(
    identifier=TableIdentifier(catalog="silver", schema="regelleistung", name="price_afrr_capacity"),
    schema=afrr_schema,
    comment="Prices for Automatic Frequency Restoration Reserve (aFRR) positive and negative capacity reserves in Germany.",
    sources=[TableIdentifier(catalog="bronze", schema="regelleistung", name="afrr_capacity_prices")],
    license=REGELLEISTUNG_LICENSE,
    tags=REGELLEISTUNG_COMMON_TAGS,
    primary_keys=("delivery_area", "delivery_start", "delivery_end", "price_direction"),
    liquid_cluster_cols=(("delivery_area", "delivery_start"),),
)

regelleistung_price_afrr_energy_table = StaticTableModel(
    identifier=TableIdentifier(catalog="silver", schema="regelleistung", name="price_afrr_energy"),
    schema=afrr_schema,
    comment="Prices for Automatic Frequency Restoration Reserve (aFRR) positive and negative activation in Germany.",
    sources=[TableIdentifier(catalog="bronze", schema="regelleistung", name="afrr_energy_prices")],
    license=REGELLEISTUNG_LICENSE,
    tags=REGELLEISTUNG_COMMON_TAGS,
    primary_keys=("delivery_area", "delivery_start", "delivery_end", "price_direction"),
    liquid_cluster_cols=(("delivery_area", "delivery_start"),),
)
