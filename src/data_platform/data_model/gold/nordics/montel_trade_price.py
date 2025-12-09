from __future__ import annotations

from pyspark.sql.types import (
    DoubleType,
    LongType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

from data_platform.data_model import FileVolumeIdentifier, StaticTableModel, TableIdentifier

license = "DNA_MONTELNEWS_DATA_LICENSE"


nordics_montel_trade_price_table = StaticTableModel(
    identifier=TableIdentifier(
        catalog="gold",
        schema="nordics",
        name="montel_trade_price",
    ),
    schema=StructType(
        [
            StructField(
                "symbol_key",
                StringType(),
                False,
                {"comment": "The symbol key of the traded contract"},
            ),
            StructField(
                "interval_start",
                TimestampType(),
                False,
                {"comment": "The timestamp when the avg starts"},
            ),
            StructField(
                "open",
                DoubleType(),
                True,
                {"comment": "The open price"},
            ),
            StructField(
                "volume_averaged_price",
                DoubleType(),
                True,
                {"comment": "The volume averaged price"},
            ),
            StructField(
                "volume",
                LongType(),
                True,
                {"comment": "The trade volume"},
            ),
            StructField(
                "high",
                DoubleType(),
                True,
                {"comment": "The high price"},
            ),
            StructField(
                "low",
                DoubleType(),
                True,
                {"comment": "The low price"},
            ),
            StructField("license", StringType(), True, {"comment": "License identifier"}),
        ]
    ),
    comment="This table contains Montel 15-min trade data for a wide range of commodity contracts.",
    sources=[
        TableIdentifier(catalog="silver", schema="montel", name="price_volume_trade"),
        FileVolumeIdentifier(catalog="staging", schema="montel", name="historical_prices"),
    ],
    license=license,
    tags={
        "confidentiality": "C2_Internal",
        "data_owner": "Knut Stensrod",
        "license": license,
        "personal_sensitive_information": "No PII",
        "source_system": "Montel",
    },
    primary_keys=("symbol_key", "interval_start"),
    partition_cols=(),
    liquid_cluster_cols=(),
)
