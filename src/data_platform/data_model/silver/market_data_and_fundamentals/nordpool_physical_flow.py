from __future__ import annotations

from pyspark.sql.types import (
    DoubleType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

from data_platform.data_model import StaticTableModel, TableIdentifier

license = "NORDPOOL_STD_NORDPOOL"


nordpool_physical_flow_table = StaticTableModel(
    identifier=TableIdentifier(
        catalog="silver",
        schema="market_data_and_fundamentals",
        name="nordpool_physical_flow",
    ),
    schema=StructType(
        [
            StructField(
                "delivery_start",
                TimestampType(),
                True,
                {"comment": "The start of the delivery period for the contract, in the format YYYY-MM-DD HH:MM:SS"},
            ),
            StructField(
                "delivery_end",
                TimestampType(),
                True,
                {"comment": "The end of the delivery period for the contract, in the format YYYY-MM-DD HH:MM:SS"},
            ),
            StructField(
                "duration",
                StringType(),
                True,
                {"comment": "The duration of the delivery period, PT1H for 1 hour, PT15M for 15 minutes, etc."},
            ),
            StructField(
                "from_area",
                StringType(),
                True,
                {"comment": "The area from which the flow is imported, e.g. SE1, FI, DK1, etc."},
            ),
            StructField(
                "to_area",
                StringType(),
                True,
                {"comment": "The area to which the flow is exported, e.g. SE1, FI, DK1, etc."},
            ),
            StructField(
                "connection",
                StringType(),
                True,
                {"comment": "The connection between the two areas , e.g. SE1-FI, DK1-DK2, etc."},
            ),
            StructField(
                "import",
                DoubleType(),
                True,
                {"comment": "The amount of flow imported from the from_area to the to_area in the specified unit."},
            ),
            StructField(
                "export",
                DoubleType(),
                True,
                {"comment": "The amount of flow exported from the to_area to the from_area in the specified unit."},
            ),
            StructField(
                "net_position",
                DoubleType(),
                True,
                {"comment": "The net position of the flow, calculated as import - export."},
            ),
            StructField("license", StringType(), True, {"comment": "License identifier"}),
            StructField(
                "updated_at_source",
                TimestampType(),
                True,
                {"comment": "The timestamp of the last update from the source"},
            ),
        ]
    ),
    comment=(
        "This table contains recalculated flows for Nordic flow-based regions. "
        "The Nordic Flow-based implementation sometimes ends up breaching physical constraints in the power grid, "
        "and a post-coupling re-calculation is done and published to the JAO publication tool, "
        "which is the source of this data. The re-calculated flows should end up with every area "
        "having the same net position (+/- ~0.1MW) as the original flows coming from Euphemia "
        "(from 30th of October 2024), but internal flows inside the regions might be re-routed. "
        "The need to re-calculate flows coming from Euphemia only exists in the Nordic flow-based regions. "
        "Scheduled physical flows are always described from an area point of view. "
        "Some flows will have a loss on the cable, imports in an area is always after losses, "
        "and exports are before losses."
    ),
    sources=[
        TableIdentifier(catalog="bronze", schema="nordpool", name="physical_flow"),
    ],
    license=license,
    tags={
        "confidentiality": "C2_Internal",
        "data_owner": "Knut Stensrod",
        "license": license,
        "personal_sensitive_information": "No PII",
        "source_system": "Nordpool",
    },
    primary_keys=("delivery_start", "from_area", "to_area"),
    partition_cols=(),
    liquid_cluster_cols=(),
)
