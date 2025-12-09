from __future__ import annotations

from pyspark.sql.types import (
    DoubleType,
    LongType,
    StringType,
    StructField,
    StructType,
)

from data_platform.data_model import StaticTableModel, TableIdentifier

ENTSOE_COMMON_TAGS = {
    "Confidentiality Level": "C2 Internal",
    "Data Domain": "Fundamental",
    "Data Owner": "Gaudenz Koeppel",
    "Licensed Data": "False",
    "Personal Identifiable Information": "No PII",
}

# Bronze schema matching the raw XML structure from ExtendedEntsoeClient
bid_balancing_energy_archives_table = StaticTableModel(
    identifier=TableIdentifier(catalog="bronze", schema="entsoe", name="bid_balancing_energy_archives"),
    schema=StructType(
        [
            StructField("document_mrid", StringType(), False, {"comment": "Document unique identifier"}),
            StructField("document_revision_number", LongType(), True, {"comment": "Document revision number"}),
            StructField("document_type", StringType(), True, {"comment": "Document type code"}),
            StructField("process_type", StringType(), True, {"comment": "Process type code (A46, A47, A51)"}),
            StructField("sender_mrid", StringType(), True, {"comment": "Sender market participant mRID"}),
            StructField("sender_role", StringType(), True, {"comment": "Sender market role type"}),
            StructField("receiver_mrid", StringType(), True, {"comment": "Receiver market participant mRID"}),
            StructField("receiver_role", StringType(), True, {"comment": "Receiver market role type"}),
            StructField("created_datetime", StringType(), True, {"comment": "Document creation timestamp"}),
            StructField("reserve_bid_period_start", StringType(), True, {"comment": "Reserve bid period start"}),
            StructField("reserve_bid_period_end", StringType(), True, {"comment": "Reserve bid period end"}),
            StructField("domain_mrid", StringType(), True, {"comment": "Domain mRID"}),
            StructField("subject_participant_mrid", StringType(), True, {"comment": "Subject market participant mRID"}),
            StructField("subject_participant_role", StringType(), True, {"comment": "Subject market participant role"}),
            StructField("bid_mrid", StringType(), False, {"comment": "Bid unique identifier"}),
            StructField("auction_mrid", StringType(), True, {"comment": "Auction mRID"}),
            StructField("business_type", StringType(), True, {"comment": "Business type"}),
            StructField("acquiring_domain_mrid", StringType(), True, {"comment": "Acquiring domain mRID"}),
            StructField("connecting_domain_mrid", StringType(), True, {"comment": "Connecting domain mRID"}),
            StructField("cancelled_ts", StringType(), True, {"comment": "Optional flag used to de-publish the Bid"}),
            StructField("quantity_measure_unit", StringType(), True, {"comment": "Quantity measure unit name"}),
            StructField("currency_unit", StringType(), True, {"comment": "Currency unit name"}),
            StructField("price_measure_unit", StringType(), True, {"comment": "Price measure unit name"}),
            StructField("divisible", StringType(), True, {"comment": "Whether bid is divisible"}),
            StructField("linked_bids_identification", StringType(), True, {"comment": "Linked bids ID"}),
            StructField("multipart_bid_identification", StringType(), True, {"comment": "Multipart bid ID"}),
            StructField("exclusive_bids_identification", StringType(), True, {"comment": "Exclusive bids ID"}),
            StructField("status", StringType(), True, {"comment": "Status value"}),
            StructField("flow_direction", StringType(), True, {"comment": "Flow direction (A01=Up, A02=Down)"}),
            StructField("standard_market_product_type", StringType(), True, {"comment": ""}),
            StructField("original_market_product_type", StringType(), True, {"comment": ""}),
            StructField("validity_period_start", StringType(), True, {"comment": "Validity period start"}),
            StructField("validity_period_end", StringType(), True, {"comment": "Validity period end"}),
            StructField("period_start", StringType(), True, {"comment": "Period start timestamp"}),
            StructField("period_end", StringType(), True, {"comment": "Period end timestamp"}),
            StructField("resolution", StringType(), True, {"comment": "Time resolution (ISO 8601 duration)"}),
            StructField("point_position", LongType(), False, {"comment": "Point position within period"}),
            StructField("quantity", DoubleType(), True, {"comment": "Quantity at this point"}),
            StructField("energy_price", DoubleType(), True, {"comment": "Energy price at this point"}),
            StructField("reason_codes", StringType(), True, {"comment": ""}),
            StructField("reason_texts", StringType(), True, {"comment": ""}),
            StructField("_source_country_code", StringType(), False, {"comment": "Country code derived from EIC code"}),
        ]
    ),
    sources=[],
    tags={**ENTSOE_COMMON_TAGS, "Data Subdomain": "Balancing and Reserve"},
    primary_keys=("document_mrid", "bid_mrid", "point_position", "period_start"),
)
