from __future__ import annotations

from pyspark.sql.types import DoubleType, LongType, StringType, StructField, StructType, TimestampType

from data_platform.data_model import StaticTableModel, TableIdentifier

from data_platform.data_model.metadata_common import standard_columns, standard_tags
from data_platform.data_model.silver.entsoe.entsoe_common import ENTSOE_COMMON_TAGS

bid_balancing_energy_archives_table = StaticTableModel(
    identifier=TableIdentifier(catalog="silver", schema="entsoe", name="bid_balancing_energy_archives"),
    schema=StructType([
        StructField("document_mrid", StringType(), False, {"comment": "Document unique identifier (GUID)"}),
        StructField("document_revision_number", LongType(), False, {"comment": "Document version number"}),
        StructField("document_type", StringType(), True, {"comment": "Document type code (A37 = Reserve Bid)"}),
        standard_columns.DeliveryStartColumn.to_struct_field(),
        standard_columns.DeliveryEndColumn.to_struct_field(),
        StructField("position", LongType(), False, {"comment": "Position within period (1-based index)"}),
        StructField("validity_start", TimestampType(), True, {"comment": "Validity period start timestamp (UTC)"}),
        StructField("validity_end", TimestampType(), True, {"comment": "Validity period end timestamp (UTC)"}),
        standard_columns.DurationColumn.to_struct_field(),
        StructField("area_display_name", StringType(), True, {"comment": "Source country code (e.g. DE, FR, IT)"}),
        StructField("acquiring_area_map_code", StringType(), True, {"comment": "Acquiring domain EIC code"}),
        StructField("connecting_area_map_code", StringType(), True, {"comment": "Connecting domain EIC code"}),
        StructField("bid_id", StringType(), False, {"comment": "Bid unique identifier"}),
        StructField("reserve_type", StringType(), True, {"comment": "Reserve type (RR, mFRR, aFRR) derived from process type"}),
        StructField("type_of_product", StringType(), True, {"comment": "Standard market product type code"}),
        StructField("direction", StringType(), True, {"comment": "Flow direction (Up, Down) derived from flow_direction code"}),
        StructField("status", StringType(), True, {"comment": "Availability status (Available, Unavailable) derived from status code"}),
        StructField("volume", DoubleType(), True, {"comment": "Offered quantity at this point"}),
        StructField("unit_volume", StringType(), True, {"comment": "Volume measurement unit (MW)"}),
        StructField("price", DoubleType(), True, {"comment": "Price per MW"}),
        standard_columns.CurrencyColumn.to_struct_field(),
        StructField("unit_price", StringType(), True, {"comment": "Price measurement unit (currency/MWh)"}),
        standard_columns.CommodityColumn.to_struct_field(),
        standard_columns.LicenseColumn.to_struct_field(),
        standard_columns.DataSourceColumn.to_struct_field(),
        standard_columns.DataSystemColumn.to_struct_field(),
    ]),
    comment="This table contains records of balancing energy bids submitted by market participants for the provision of balancing services. Each entry details the characteristics of a balancing energy bid, including the product type, bid volume, price, and relevant time periods. The table supports transparency and analysis of the balancing energy market by providing insight into the supply side of balancing mechanisms. The data is sourced from ENTSO-E Balancing energy bids (12.3.B & C)",
    sources=[TableIdentifier(catalog="bronze", schema="entsoe", name="bid_balancing_energy_archives")],
    tags={
        **ENTSOE_COMMON_TAGS,
         standard_tags.DataSubdomain.KEY: standard_tags.DataSubdomain.BALANCING_AND_RESERVE
         },
    primary_keys=("document_mrid", "bid_id", "delivery_start"),
)
