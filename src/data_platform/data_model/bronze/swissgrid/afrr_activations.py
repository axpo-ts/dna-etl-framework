from pyspark.sql.types import (
    FloatType,
    StringType,
    StructField,
    StructType,
)

from data_platform.data_model import FileVolumeIdentifier, StaticTableModel, TableIdentifier

afrr_activations_table = StaticTableModel(
    identifier=TableIdentifier("bronze", "swissgrid", "afrr_activations"),
    schema=StructType(
        [
            StructField("DateTimeFrom", StringType(), True),
            StructField("Volume", FloatType(), True),
            StructField("_rescued_data", StringType(), True),
            StructField("file_path", StringType(), True),
        ]
    ),
    comment="aFRR activations files.",
    sources=[
        FileVolumeIdentifier(
            catalog="staging",
            schema="swissgrid",
            name="afrr_activations",
            file_path=None,
        )
    ],
    primary_keys=("DateTimeFrom", "Volume"),
)
