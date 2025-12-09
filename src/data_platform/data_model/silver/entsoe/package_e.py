from __future__ import annotations

from pyspark.sql.types import (
    DoubleType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

from data_platform.data_model import StaticTableModel, TableIdentifier
from data_platform.data_model.metadata_common import standard_columns, standard_tags
from data_platform.data_model.silver.entsoe.entsoe_common import ENTSOE_COMMON_TAGS, entsoe_standard_columns

# Bid Balancing Energy Table
bid_balancing_energy_table = StaticTableModel(
    identifier=TableIdentifier(catalog="silver", schema="entsoe", name="bid_balancing_energy"),
    schema=StructType(
        [
            StructField("instance_code", StringType(), False, {"comment": "A unique code identifying a particular instance"}),
            standard_columns.DeliveryStartColumn.to_struct_field(),
            standard_columns.DeliveryEndColumn.to_struct_field(),
            StructField("validity_start", TimestampType(), True, {"comment": "The timestamp of the start of the validity period in UTC"}),
            StructField("validity_end", TimestampType(), True, {"comment": "The timestamp of the end of the validity period in UTC"}),
            StructField("isp_timestamp", TimestampType(), True, {"comment": "The imbalance settlement period start in UTC"}),
            standard_columns.DurationColumn.to_struct_field(),
            entsoe_standard_columns.AreaCodeColumn.to_struct_field(),
            StructField("area_display_name", StringType(), True, {"comment": "The name of the geographic area, market zone, or control area relevant to the data record"}),
            entsoe_standard_columns.AreaTypeCodeColumn.to_struct_field(),
            entsoe_standard_columns.AreaMapCodeColumn.to_struct_field(),
            StructField("bid_id", StringType(), False, {"comment": "Bid identifier"}),
            StructField("reserve_type", StringType(), True, {"comment": "The category of ancillary service or reserve product to which the data record pertains, e.g. FCR, aFRR"}),
            StructField("type_of_product", StringType(), True, {"comment": "Product type"}),
            StructField("direction", StringType(), True, {"comment": "Bid direction (Up/Down)"}),
            StructField("complexity", StringType(), True, {"comment": "Bid complexity"}),
            StructField("status", StringType(), True, {"comment": "Bid status"}),
            StructField("volume", DoubleType(), True, {"comment": "Volume in MW"}),
            StructField("unit_volume", StringType(), True, {"comment": "The unit of measurement of volume"}),
            StructField("price", DoubleType(), True, {"comment": "Price per MWh"}),
            standard_columns.CurrencyColumn.to_struct_field(),
            StructField("unit_price", StringType(), True, {"comment": "Unit of measurement: Currency/MWh"}),
            StructField("reason_for_unavailability", StringType(), True, {"comment": "Reason for unavailability"}),
            StructField("reason_for_unavailability_note", StringType(), True, {"comment": "Reason for unavailability note"}),
            StructField("activation_purpose", StringType(), True, {"comment": "Activation purpose"}),
            StructField("activation_purpose_note", StringType(), True, {"comment": "Activation purpose note"}),
            standard_columns.CommodityColumn.to_struct_field(),
            standard_columns.LicenseColumn.to_struct_field(),
            standard_columns.DataSourceColumn.to_struct_field(),
            standard_columns.DataSystemColumn.to_struct_field(),
            standard_columns.UpdatedAtSourceColumn.to_struct_field(),
        ]
    ),
    comment="This table contains records of balancing energy bids submitted by market participants for the provision of balancing services. Each entry details the characteristics of a balancing energy bid, including the product type, bid volume, price, and relevant time periods. The table supports transparency and analysis of the balancing energy market by providing insight into the supply side of balancing mechanisms. The data is sourced from ENTSO-E Balancing energy bids (12.3.B & C)",
    sources=[
        TableIdentifier(catalog="bronze", schema="entsoe", name="balancingenergybids_12_3_b_c_r3"),
    ],
    tags={**ENTSOE_COMMON_TAGS, standard_tags.DataSubdomain.KEY: standard_tags.DataSubdomain.BALANCING_AND_RESERVE},
    primary_keys=("instance_code", "delivery_start", "delivery_end", "isp_timestamp", "updated_at_source"),
)
