from __future__ import annotations

from pyspark.sql.types import DoubleType, IntegerType, StringType, StructField, StructType

from data_platform.data_model import StaticTableModel, TableIdentifier

from .rte_common import RTE_COMMON_TAGS, RTE_LICENSE

afrr_energy_activation_invoicing_table = StaticTableModel(
    identifier=TableIdentifier("silver", "rte", "afrr_energy_activation_invoicing"),
    schema=StructType(
        [
            StructField("start_date", StringType(), True),
            StructField("end_date", StringType(), True),
            StructField("total_page", IntegerType(), True),
            StructField("resolution", StringType(), True),
            StructField("period_day", StringType(), True),
            StructField("edr_code", StringType(), True),
            StructField("position", IntegerType(), True),
            StructField("energy_up", DoubleType(), True),
            StructField("energy_down", DoubleType(), True),
            StructField("price_up", DoubleType(), True),
            StructField("price_down", DoubleType(), True),
            StructField("remuneration_price_up", DoubleType(), True),
            StructField("remuneration_price_down", DoubleType(), True),
            StructField("update_date", StringType(), True),
            StructField("revision_number", IntegerType(), True),
        ]
    ),
    comment="Invoicing data for automatic Frequency Restoration Reserve (aFRR) energy activation from RTE (French TSO)",
    license=RTE_LICENSE,
    tags=RTE_COMMON_TAGS,
    primary_keys=("start_date", "end_date", "resolution", "period_day", "edr_code", "position", "update_date"),
)
