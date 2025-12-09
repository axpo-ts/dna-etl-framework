from __future__ import annotations

from pyspark.sql.types import DoubleType, IntegerType, StringType, StructField, StructType, TimestampType

from data_platform.data_model import StaticTableModel, TableIdentifier
from data_platform.data_model.metadata_common import standard_columns, standard_tags

bid_transmission_capacity_auction_table = StaticTableModel(
    identifier=TableIdentifier(catalog="silver", schema="jao", name="bid_transmission_capacity_auction"),
    schema=StructType([
        StructField("auction_id", StringType(), True, {"comment": "The id of the auction"}),
        StructField("delivery_start", TimestampType(), True, {"comment": "Start timestamp of the capacity (aggregated from product_hour & auction id). DST shifts are already accounted for."}),
        StructField("delivery_end", TimestampType(), True, {"comment": "End timestamp of the capacity (aggregated from product_hour & auction id). DST shifts are already accounted for."}),
        StructField("duration", StringType(), True, {"comment": "Duration of the product in hours"}),
        StructField("corridor_code", StringType(), True, {"comment": "Combined raw code for between which countries and through which cable the capacity applies for"}),
        StructField("from_area", StringType(), True, {"comment": "The source area the capacity applies for"}),
        StructField("to_area", StringType(), True, {"comment": "The target area the capacity applies for"}),
        StructField("product_identification", StringType(), True, {"comment": "Identifier of the product / hour"}),
        StructField("product_minutes_delivered", IntegerType(), True, {"comment": "Number of minutes delivered in the product"}),
        StructField("product_hour", StringType(), True, {"comment": "Hour range the bid applies for (HH:MM-HH:MM). Flat spots and DST shifts are encoded as 00:00-00:00"}),
        standard_columns.CurrencyColumn.to_struct_field(),
        StructField("price", DoubleType(), True, {"comment": "The price bid by the participant"}),
        StructField("awarded_price", DoubleType(), True, {"comment": "The price awarded in the auction result"}),
        StructField("quantity", DoubleType(), True, {"comment": "Quantity offered in the bid"}),
        StructField("awarded_quantity", DoubleType(), True, {"comment": "Quantity awarded to the bidder"}),
        StructField("resold_quantity", DoubleType(), True, {"comment": "Quantity returned for reallocation"}),
        StructField("cable_info", StringType(), True, {"comment": "The cable of which the capacity applies for"}),
        standard_columns.UpdatedAtSourceColumn.to_struct_field(),
        standard_columns.CommodityColumn.to_struct_field(),
        standard_columns.LicenseColumn.to_struct_field(),
        standard_columns.DataSourceColumn.to_struct_field(),
        standard_columns.DataSystemColumn.to_struct_field(),
    ]),
    comment="Bid-level results from JAO long-term transmission right auctions.",
    sources=[TableIdentifier(catalog="bronze", schema="jao", name="bids")],
    primary_keys=("updated_at_source", "auction_id", "product_hour"),
    tags={
        standard_tags.ConfidentialityLevel.KEY: standard_tags.ConfidentialityLevel.C2_INTERNAL,
        standard_tags.DataDomain.KEY: standard_tags.DataDomain.FUNDAMENTAL,
        standard_tags.DataOwner.KEY: standard_tags.DataOwner.KNUT_STENSROD,
        standard_tags.DataSubdomain.KEY: standard_tags.DataSubdomain.TRANSMISSION,
        standard_tags.LicensedData.KEY: standard_tags.LicensedData.FALSE,
        standard_tags.PIIClassification.KEY: standard_tags.PIIClassification.NO_PII,
    },
)
