from pyspark.sql.types import DecimalType, DoubleType, StringType, StructField, StructType, TimestampType

from data_platform.data_model import StaticTableModel, TableIdentifier

measure_table = StaticTableModel(
    identifier=TableIdentifier(
        catalog="bronze",
        schema="neuron",
        name="falcon_measure",
    ),
    schema=StructType(
        [
            StructField(
                "DATE",
                TimestampType(),
                True,
                {},
            ),
            StructField(
                "CUPS20",
                StringType(),
                True,
                {},
            ),
            StructField(
                "CUPS22",
                StringType(),
                True,
                {},
            ),
            StructField(
                "LINE",
                StringType(),
                True,
                {},
            ),
            StructField(
                "PRIORITY",
                StringType(),
                True,
                {},
            ),
            StructField(
                "TYPE",
                StringType(),
                True,
                {},
            ),
            StructField(
                "SUBTYPE",
                StringType(),
                True,
                {},
            ),
            StructField(
                "UNIT",
                StringType(),
                True,
                {},
            ),
            StructField(
                "FREQUENCY",
                StringType(),
                True,
                {},
            ),
            StructField(
                "H_ORDER",
                DecimalType(38, 0),
                True,
                {},
            ),
            StructField(
                "HOUR",
                DecimalType(38, 0),
                True,
                {},
            ),
            StructField(
                "TIMESTAMP_CREATION",
                TimestampType(),
                True,
                {},
            ),
            StructField(
                "TIMESTAMP_UPDATE",
                TimestampType(),
                True,
                {},
            ),
            StructField(
                "VALUE",
                DoubleType(),
                True,
                {},
            ),
            StructField(
                "SRC_CREATED_BY",
                StringType(),
                True,
                {},
            ),
            StructField(
                "SRC_CREATED_AT",
                TimestampType(),
                True,
                {},
            ),
        ]
    ),
    comment="Consumption measurement data for electricity supply points in the Spanish market.",
    sources=(TableIdentifier(catalog="neuron_aa_prod", schema="falcon_aws", name="measure"),),
    primary_keys=(
        "DATE",
        "CUPS22",
        "PRIORITY",
        "TYPE",
        "SUBTYPE",
        "UNIT",
        "FREQUENCY",
        "H_ORDER",
        "TIMESTAMP_UPDATE",
    ),
    tags={
        "License": "Licence",
        "Confidentiality Level": "C2_Internal",
        "Personal sensitive information": "No PII",
        "Source System": "Neuron",
        "LPI": "Falcon_AXPO-IBERIA_Neuron",
        "Data Owner": "Ignacio Ortiz de Zuniga",
    },
)
