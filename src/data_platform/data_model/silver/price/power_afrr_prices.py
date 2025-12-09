from __future__ import annotations

from pyspark.sql.types import (
    DoubleType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

from data_platform.data_model import StaticTableModel, TableIdentifier

# Define the common schema for both tables
afrr_schema = StructType(
    [
        StructField("delivery_area", StringType(), False, {"comment": "Delivery area code (e.g., BE, DE, NL)"}),
        StructField("delivery_start", TimestampType(), False, {"comment": "Start time of delivery period"}),
        StructField("delivery_end", TimestampType(), False, {"comment": "End time of delivery period"}),
        StructField("price_direction", StringType(), False, {"comment": "Price direction (UP or DOWN)"}),
        StructField("average_price", DoubleType(), True, {"comment": "Average price in EUR/MWh"}),
        StructField("marginal_price", DoubleType(), True, {"comment": "Marginal price in EUR/MWh"}),
        StructField("resolution_iso", StringType(), True, {"comment": "ISO 8601 duration for resolution"}),
        StructField("unit", StringType(), False, {"comment": "Unit of measurement (EUR/MWh)"}),
        StructField("data_source", StringType(), False, {"comment": "Data source name"}),
        StructField("source_system", StringType(), False, {"comment": "Source system name"}),
        StructField("license", StringType(), False, {"comment": "License identifier"}),
    ]
)

power_afrr_energy_table = StaticTableModel(
    identifier=TableIdentifier(
        catalog="silver",
        schema="price",
        name="power_afrr_energy",
    ),
    schema=afrr_schema,
    comment="Automatic Frequency Restoration Reserve (aFRR) energy prices from multiple European TSOs",
    sources=[
        TableIdentifier(catalog="bronze", schema="elia", name="afrr_energy_price"),
        TableIdentifier(catalog="bronze", schema="regelleistung", name="afrr_energy_prices"),
        TableIdentifier(catalog="bronze", schema="tennet", name="merit_order_list"),
    ],
    tags={
        "data_owner": "Christophe Cattry",
        "license": "License",
        "lpi": "DNA-ALL-ACCESS",
        "personal sensitive information": "No PII",
        "source system": "Elia,Regelleistung,TenneT",
    },
    primary_keys=("delivery_area", "delivery_start", "delivery_end", "price_direction"),
    liquid_cluster_cols=(("delivery_area", "delivery_start"),),
)

power_afrr_capacity_table = StaticTableModel(
    identifier=TableIdentifier(
        catalog="silver",
        schema="price",
        name="power_afrr_capacity",
    ),
    schema=afrr_schema,
    comment="Automatic Frequency Restoration Reserve (aFRR) capacity prices from multiple European TSOs",
    sources=[
        TableIdentifier(catalog="bronze", schema="elia", name="afrr_capacity_price"),
        TableIdentifier(catalog="bronze", schema="regelleistung", name="afrr_capacity_prices"),
    ],
    tags={
        "data_owner": "Christophe Cattry",
        "license": "License",
        "lpi": "DNA-ALL-ACCESS",
        "personal sensitive information": "No PII",
        "source system": "Elia,Regelleistung",
    },
    primary_keys=("delivery_area", "delivery_start", "delivery_end", "price_direction"),
    liquid_cluster_cols=(("delivery_area", "delivery_start"),),
)
