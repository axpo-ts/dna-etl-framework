from pyspark.sql.types import DecimalType, DoubleType, StringType, StructField, StructType, TimestampType

from data_platform.data_model import StaticTableModel, TableIdentifier

best_measure_table = StaticTableModel(
    identifier=TableIdentifier(
        catalog="bronze",
        schema="neuron",
        name="falcon_best_measure",
    ),
    schema=StructType(
        [
            StructField("DATE", TimestampType(), True, {}),
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
            StructField("SOURCE", StringType(), True, {}),
            StructField("TYPE", StringType(), True, {}),
            StructField("SUBTYPE", StringType(), True, {}),
            StructField("UNIT", StringType(), True, {}),
            StructField("FREQUENCY", StringType(), True, {}),
            StructField("MARKET", StringType(), True, {}),
            StructField("VALUE", DoubleType(), True, {}),
            StructField("VALUE_SOURCE", StringType(), True, {}),
            StructField(
                "TIME_PERIOD",
                DecimalType(38, 0),
                True,
                {},
            ),
            StructField(
                "TIME_PERIOD_ORDER",
                DecimalType(38, 0),
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
                "TIMESTAMP_CREATION",
                TimestampType(),
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
    comment="Best measurement data from Neuron AA system (best available values for each point in time)",
    sources=(TableIdentifier(catalog="neuron_aa_prod", schema="falcon_aws", name="best_measure"),),
    primary_keys=(
        "DATE",
        "CUPS22",
        "TYPE",
        "SUBTYPE",
        "UNIT",
        "FREQUENCY",
        "VALUE_SOURCE",
        "TIME_PERIOD_ORDER",
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
