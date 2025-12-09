"""Nordpool Flow-Based Constraints bronze table model."""

from __future__ import annotations

from pyspark.sql.types import (
    ArrayType,
    DoubleType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

from data_platform.data_model import FileVolumeIdentifier, StaticTableModel, TableIdentifier

flow_based_constraints_table = StaticTableModel(
    identifier=TableIdentifier(catalog="bronze", schema="nordpool", name="flow_based_constraints"),
    schema=StructType(
        [
            StructField("deliveryDateCET", StringType(), True, {"comment": "Delivery date in CET format"}),
            StructField("updatedAt", TimestampType(), True, {"comment": "Timestamp when data was updated"}),
            StructField("market", StringType(), True, {"comment": "Market type (e.g., DayAhead)"}),
            StructField("status", StringType(), True, {"comment": "Status of the data"}),
            StructField(
                "flowBasedDomain", StringType(), True, {"comment": "Flow-based domain (e.g., Core, Nordic, DK1)"}
            ),
            StructField(
                "remainingAvailableMarginUnit",
                StringType(),
                True,
                {"comment": "Unit for remaining available margin"},
            ),
            StructField(
                "periods",
                ArrayType(
                    StructType(
                        [
                            StructField(
                                "deliveryStart", TimestampType(), True, {"comment": "Period delivery start time"}
                            ),
                            StructField("deliveryEnd", TimestampType(), True, {"comment": "Period delivery end time"}),
                            StructField(
                                "constraints",
                                ArrayType(
                                    StructType(
                                        [
                                            StructField("id", StringType(), True, {"comment": "Constraint identifier"}),
                                            StructField("name", StringType(), True, {"comment": "Constraint name"}),
                                            StructField(
                                                "remainingAvailableMargin",
                                                DoubleType(),
                                                True,
                                                {"comment": "Remaining available margin value"},
                                            ),
                                            StructField(
                                                "powerTransferDistributionFactors",
                                                ArrayType(
                                                    StructType(
                                                        [
                                                            StructField(
                                                                "area",
                                                                StringType(),
                                                                True,
                                                                {"comment": "Area identifier"},
                                                            ),
                                                            StructField(
                                                                "powerTransferDistributionFactor",
                                                                DoubleType(),
                                                                True,
                                                                {"comment": "PTDF value"},
                                                            ),
                                                        ]
                                                    )
                                                ),
                                                True,
                                                {"comment": "Power transfer distribution factors"},
                                            ),
                                        ]
                                    )
                                ),
                                True,
                                {"comment": "List of constraints for the period"},
                            ),
                        ]
                    )
                ),
                True,
                {"comment": "List of delivery periods"},
            ),
            StructField("file_name", StringType(), True, {"comment": "Source file name"}),
        ]
    ),
    comment="Nordpool Flow-Based Constraints data from Market Data API",
    primary_keys=("deliveryDateCET", "flowBasedDomain", "market"),
    sources=[FileVolumeIdentifier(catalog="staging", schema="nordpool", name="flow_based_constraints")],
)
