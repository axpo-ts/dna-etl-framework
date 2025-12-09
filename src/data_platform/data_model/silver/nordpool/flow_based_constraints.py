from __future__ import annotations

from pyspark.sql.types import (
    DoubleType,
    StringType,
    StructField,
    StructType,
)

from data_platform.data_model import StaticTableModel, TableIdentifier
from data_platform.data_model.metadata_common import standard_columns, standard_tags
from data_platform.data_model.silver.nordpool.nordpool_common import (
    NORDPOOL_COMMON_TAGS,
    NORDPOOL_LICENSE,
)

flow_based_constraints_table = StaticTableModel(
    identifier=TableIdentifier(catalog="silver", schema="nordpool", name="flow_based_constraints"),
    schema=StructType([
        StructField("delivery_date_cet", StringType(), False, {"comment": "Delivery date in CET format"}),
        standard_columns.UpdatedAtSourceColumn.to_struct_field(nullable=False, comment="Timestamp when data was updated at source"),
        StructField("market", StringType(), False, {"comment": "Market type (e.g., DayAhead)"}),
        StructField("status", StringType(), True, {"comment": "Status of the data"}),
        StructField("flow_based_domain", StringType(), False, {"comment": "Flow-based domain (e.g., Core)"}),
        StructField("remaining_available_margin_unit", StringType(), True, {"comment": "Unit for remaining available margin"}),
        standard_columns.DeliveryStartColumn.to_struct_field(nullable=False, comment="Period delivery start time"),
        standard_columns.DeliveryEndColumn.to_struct_field(nullable=False, comment="Period delivery end time"),
        StructField("constraint_id", StringType(), False, {"comment": "Constraint identifier"}),
        StructField("constraint_name", StringType(), True, {"comment": "Constraint name"}),
        StructField("remaining_available_margin", DoubleType(), True, {"comment": "Remaining available margin value"}),
        StructField("area", StringType(), True, {"comment": "Area identifier for PTDF"}),
        StructField("ptdf", DoubleType(), True, {"comment": "PTDF value"}),
    ]),
    comment="Nordpool Flow-Based Constraints data from Market Data API",
    primary_keys=("delivery_date_cet", "updated_at_source", "flow_based_domain", "market", "delivery_start", "delivery_end", "constraint_id"),
    sources=[TableIdentifier(catalog="bronze", schema="nordpool", name="flow_based_constraints")],
    license=NORDPOOL_LICENSE,
    tags={
        **NORDPOOL_COMMON_TAGS,
        standard_tags.DataDomain.KEY: standard_tags.DataDomain.FUNDAMENTAL,
        standard_tags.DataSubdomain.KEY: standard_tags.DataSubdomain.TRANSMISSION,
    }
)
