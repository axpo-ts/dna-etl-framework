from pyspark.sql.types import DateType, DecimalType, DoubleType, StringType, StructField, StructType, TimestampType

from data_platform.data_model import StaticTableModel, TableIdentifier

agregaciones_table = StaticTableModel(
    identifier=TableIdentifier(
        catalog="bronze",
        schema="neuron",
        name="falcon_aggregations",
    ),
    schema=StructType(
        [
            StructField("CUPS20", StringType(), True, {}),
            StructField("HOUR", DecimalType(38, 0), True, {}),
            StructField("H_ORDER", DecimalType(38, 0), True, {}),
            StructField("FARE", StringType(), True, {}),
            StructField(
                "UNIDAD_DE_PROGRAMACION",
                StringType(),
                True,
                {},
            ),
            StructField("ZONA_GEOGRAFICA", StringType(), True, {}),
            StructField("DENOMINACION_SOCIAL", StringType(), True, {}),
            StructField("CIFNIF", StringType(), True, {}),
            StructField("PROVINCIA", StringType(), True, {}),
            StructField(
                "CNAE",
                StringType(),
                True,
                {},
            ),
            StructField(
                "LIQUIDADA_ACTIVA",
                DoubleType(),
                True,
                {},
            ),
            StructField(
                "LIQUIDADA_LOSS",
                DoubleType(),
                True,
                {},
            ),
            StructField(
                "MM_ACTIVA",
                DoubleType(),
                True,
                {},
            ),
            StructField(
                "MM_LOSS",
                DoubleType(),
                True,
                {},
            ),
            StructField("FORECAST_ACTIVA", DoubleType(), True, {}),
            StructField("FORECAST_LOSS", DoubleType(), True, {}),
            StructField(
                "FORECAST_ORIGINAL_ACTIVA",
                DoubleType(),
                True,
                {},
            ),
            StructField(
                "FORECAST_ORIGINAL_LOSS",
                DoubleType(),
                True,
                {},
            ),
            StructField("ACTIVA_REE", DoubleType(), True, {}),
            StructField("DATE", DateType(), True, {}),
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
    comment="Energy aggregations data from Neuron AA system",
    sources=(TableIdentifier(catalog="neuron_aa_prod", schema="falcon_aws", name="agregaciones"),),
    primary_keys=("CUPS20", "DATE", "H_ORDER", "CIFNIF", "SRC_CREATED_AT"),
    tags={
        "License": "Licence",
        "Confidentiality Level": "C2_Internal",
        "Personal sensitive information": "No PII",
        "Source System": "Neuron",
        "LPI": "Falcon_AXPO-IBERIA_Neuron",
        "Data Owner": "Ignacio Ortiz de Zuniga",
    },
)
