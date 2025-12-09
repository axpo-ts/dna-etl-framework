from pyspark.sql.types import DecimalType, StringType, StructField, StructType

from data_platform.data_model import FileVolumeIdentifier, StaticTableModel, TableIdentifier

activation_signal_table = StaticTableModel(
    identifier=TableIdentifier("bronze", "swissgrid", "activation_signal"),
    schema=StructType(
        [
            StructField("DatumZeit", StringType(), True),
            StructField("Stellsignal_abs_aktiv", DecimalType(10, 4), True),
            StructField("_rescued_data", StringType(), True),
            StructField("file_path", StringType(), True),
        ]
    ),
    comment="activation_signals table comment",
    sources=[
        FileVolumeIdentifier(
            catalog="staging",
            schema="swissgrid",
            name="activation_signal",
            file_path=None,
        )
    ],
    primary_keys=("DatumZeit", "Stellsignal_abs_aktiv"),
)
