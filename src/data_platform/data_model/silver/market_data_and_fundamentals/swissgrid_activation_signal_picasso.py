from __future__ import annotations

from pyspark.sql import functions as f
from pyspark.sql.types import (
    FloatType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

from data_platform.data_model import StaticTableModel, TableIdentifier

swissgrid_activation_signal_picasso_table = StaticTableModel(
    identifier=TableIdentifier(
        catalog="silver",
        schema="market_data_and_fundamentals",
        name="swissgrid_activation_signal_picasso",
    ),
    schema=StructType(
        [
            StructField("time_stamp_utc", TimestampType(), False, {"comment": "Delivery start time"}),
            StructField("volume", FloatType(), True, {"comment": "Secondary energy demand"}),
            StructField("unit_type", StringType(), True, {"comment": "Unit of measurement"}),
            StructField("license", StringType(), True, {"comment": "License identifier"}),
        ]
    ),
    comment="Holds activation signals from Picasso",
    sources=[
        TableIdentifier(
            catalog="bronze",
            schema="swissgrid",
            name="activation_signal",
        ),
        TableIdentifier(
            catalog="bronze",
            schema="swissgrid",
            name="afrr_activations",
        ),
    ],
    license=f.lit("PICASSO_LICENSE"),
    tags={
        "confidentiality level": "C2_internal",
        "data_owner": "David Perraudin",
        "license": "License",
        "lpi": "DNA-ALL-ACCESS",
        "personal sensitive information": "No PII",
        "source system": "Swissgrid",
    },
    primary_keys=("time_stamp_utc",),
    partition_cols=(),
    liquid_cluster_cols=(),
)
