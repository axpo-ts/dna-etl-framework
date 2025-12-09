from __future__ import annotations

from pyspark.sql.types import (
    ArrayType,
    BooleanType,
    DoubleType,
    IntegerType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

from data_platform.data_model import StaticTableModel, TableIdentifier

license = "DNA-ALL-ACCESS"
jao_auction_table = StaticTableModel(
    identifier=TableIdentifier(
        catalog="silver",
        schema="market_data_and_fundamentals",
        name="jao_auction",
    ),
    schema=StructType(
        [
            # ───────── basic identifiers ─────────────────────────────
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
            # ───────── time columns ─────────────────────────────────
            StructField(
                "delivery_start",
                TimestampType(),
                True,
                {
                    "comment": "Start timestamp of the capacity (aggregated from product_hour & auction date)."
                    " DST shifts are already accounted for."
                },
            ),
            StructField(
                "delivery_end",
                TimestampType(),
                True,
                {
                    "comment": "End timestamp of the capacity (aggregated from product_hour & auction date)."
                    " DST shifts are already accounted for."
                },
            ),
            StructField(
                "schedule_type",
                StringType(),
                True,
                {"comment": "Schedule info (e.g. OnPeak, Everyday) taken from product_hour"},
            ),
            StructField(
                "ftroption",
                StringType(),
                True,
                {"comment": "Indicates whether FTR option applies (financial compensation on resale)"},
            ),
            # ───────── auction meta ────────────────────────────────
            StructField("auction_id", StringType(), True, {"comment": "Identifier of the auction"}),
            StructField("auction_close", TimestampType(), True, {"comment": "Closing timestamp of the auction"}),
            StructField(
                "horizon_name", StringType(), True, {"comment": "Time horizon: Intraday, Daily, Weekly, Monthly"}
            ),
            # ───────── maintenances ────────────────────────────────
            StructField(
                "maintenances",
                ArrayType(
                    StructType(
                        [
                            StructField("corridor_code", StringType(), True),
                            StructField("offered_capacity", DoubleType(), True),
                            StructField("atc", DoubleType(), True),
                            StructField("period_start", TimestampType(), True),
                            StructField("period_stop", TimestampType(), True),
                            StructField("delivery_minutes", DoubleType(), True),
                            StructField("reduced_offered_capacity", DoubleType(), True),
                        ]
                    )
                ),
                True,
                {"comment": "Potential maintenance periods on the connection"},
            ),
            # ───────── result figures ───────────────────────────────
            StructField("result_comment", StringType(), True, {"comment": "Comments on the auction result"}),
            StructField("offered_capacity", DoubleType(), True, {"comment": "Offered capacity on the auction"}),
            StructField("requested_capacity", DoubleType(), True, {"comment": "Requested capacity on the auction"}),
            StructField("allocated_capacity", DoubleType(), True, {"comment": "Allocated capacity on the auction"}),
            StructField("product_id", StringType(), True, {"comment": "Product identifier denoting the hour"}),
            StructField("auction_price", DoubleType(), True, {"comment": "Resulting price of the auction"}),
            StructField(
                "result_additional_message", StringType(), True, {"comment": "Additional messages for participants"}
            ),
            StructField("xn_rule", StringType(), True, {"comment": "Number of installments included in first invoice"}),
            StructField(
                "winning_parties_eic_code", ArrayType(StringType()), True, {"comment": "EIC codes of winning parties"}
            ),
            StructField(
                "operational_message", StringType(), True, {"comment": "Formal communication about significant events"}
            ),
            # ───────── product-level figures ────────────────────────
            StructField(
                "product_comment", StringType(), True, {"comment": "Additional information for specific products"}
            ),
            StructField("bid_party_count", IntegerType(), True, {"comment": "Number of bidders on the auction"}),
            StructField(
                "product_offered_capacity", DoubleType(), True, {"comment": "Offered capacity for the product"}
            ),
            StructField("product_atc", DoubleType(), True, {"comment": "Available transmission capacity (ATC)"}),
            StructField(
                "product_resold_capacity",
                DoubleType(),
                True,
                {"comment": "Previously allocated LTTRs returned for reallocation"},
            ),
            StructField(
                "product_winner_party_count",
                IntegerType(),
                True,
                {"comment": "Number of winning parties for the product"},
            ),
            # ───────── status / meta ────────────────────────────────
            StructField("cancelled", BooleanType(), True, {"comment": "True if the auction was cancelled"}),
            StructField(
                "updated_at_source",
                TimestampType(),
                True,
                {"comment": "Timestamp when the record was created in the source system"},
            ),
            StructField("license", StringType(), True, {"comment": "License identifier"}),
        ]
    ),
    comment="Auction data from JAO on long-term transmission rights (LTTRs). "
    "delivery_start / delivery_end are derived from product_hour and auction date. "
    "For OnPeak / Everyday products hours are normalised to 00:00; DST shifts handled.",
    sources=[
        TableIdentifier(catalog="bronze", schema="jao", name="auction"),
    ],
    license=license,
    tags={
        "confidentiality": "C2_Internal",
        "data_owner": "Knut Stensrod",
        "license": license,
        "personal_sensitive_information": "No PII",
        "source_system": "JAO",
    },
    primary_keys=("delivery_start", "delivery_end", "corridor_code"),
    partition_cols=(),
    liquid_cluster_cols=(),
)
