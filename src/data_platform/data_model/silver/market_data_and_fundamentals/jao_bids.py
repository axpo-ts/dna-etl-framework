# filepath: /home/jovyan/dna-platform-etl/src/data_platform/data_model/silver/market_data_and_fundamentals/jao_bids.py
from __future__ import annotations

from pyspark.sql.types import (
    DoubleType,
    IntegerType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

from data_platform.data_model import StaticTableModel, TableIdentifier

license = "DNA-ALL-ACCESS"


jao_bids_table = StaticTableModel(
    identifier=TableIdentifier(
        catalog="silver",
        schema="market_data_and_fundamentals",
        name="jao_bids",
    ),
    schema=StructType(
        [
            StructField(
                "corridor_code",
                StringType(),
                True,
                {
                    "comment": "Combined raw code for between which countries and through which cable the capacity applies for"
                },
            ),
            StructField("from_area", StringType(), True, {"comment": "The source area the capacity applies for"}),
            StructField("to_area", StringType(), True, {"comment": "The target area the capacity applies for"}),
            StructField("cable_info", StringType(), True, {"comment": "The cable of which the capacity applies for"}),
            StructField("auction_id", StringType(), True, {"comment": "The id of the auction"}),
            StructField(
                "product_hour",
                StringType(),
                True,
                {
                    "comment": "Hour range the bid applies for (HH:MM-HH:MM). "
                    "Flat spots and DST shifts are encoded as 00:00-00:00."
                },
            ),
            StructField("currency", StringType(), True, {"comment": "The currency of the bid"}),
            StructField("price", DoubleType(), True, {"comment": "The price bid by the participant"}),
            StructField("awarded_price", DoubleType(), True, {"comment": "The price awarded in the auction result"}),
            StructField("quantity", DoubleType(), True, {"comment": "Quantity offered in the bid"}),
            StructField("awarded_quantity", DoubleType(), True, {"comment": "Quantity awarded to the bidder"}),
            StructField("resold_quantity", DoubleType(), True, {"comment": "Quantity returned for reallocation"}),
            StructField("product_identification", StringType(), True, {"comment": "Identifier of the product / hour"}),
            StructField(
                "product_minutes_delivered",
                IntegerType(),
                True,
                {"comment": "Number of minutes delivered in the product"},
            ),
            StructField("license", StringType(), True, {"comment": "License identifier"}),
            StructField("updated_at_source", TimestampType(), True, {"comment": "Last update timestamp from source"}),
        ]
    ),
    comment="Bid-level results from JAO long-term transmission right auctions.",
    sources=[
        TableIdentifier(catalog="bronze", schema="jao", name="bids"),
    ],
    license=license,
    tags={
        "confidentiality": "C2_Internal",
        "data_owner": "Knut Stensrod",
        "license": license,
        "personal_sensitive_information": "No PII",
        "source_system": "JAO",
    },
    primary_keys=("updated_at_source", "auction_id", "product_hour"),
    partition_cols=(),
    liquid_cluster_cols=(),
)
