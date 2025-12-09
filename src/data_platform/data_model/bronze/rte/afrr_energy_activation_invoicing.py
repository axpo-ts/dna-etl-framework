from pyspark.sql.types import (
    DoubleType,
    IntegerType,
    StringType,
    StructField,
    StructType,
)

from data_platform.data_model import FileVolumeIdentifier, StaticTableModel, TableIdentifier

afrr_energy_activation_invoicing_table = StaticTableModel(
    identifier=TableIdentifier("bronze", "rte", "afrr_energy_activation_invoicing"),
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
            StructField("file_path", StringType(), True),
        ]
    ),
    sources=[FileVolumeIdentifier(catalog="staging", schema="rte", name="afrr_energy_activation_invoicing")],
    primary_keys=("start_date", "end_date", "resolution", "period_day", "edr_code", "position", "update_date"),
)
