from pyspark.sql.types import StringType, StructField, StructType

from data_platform.data_model import StaticTableModel, TableIdentifier
from data_platform.data_model.metadata_common import standard_tags
from data_platform.data_model.silver.reference.reference_common import REFERENCE_COMMON_TAGS

currency_table = StaticTableModel(
    identifier=TableIdentifier(
        catalog="silver",
        schema="reference",
        name="currency"
    ),
    schema=StructType([
        StructField("pk_currency", StringType(), False, {"comment": "A unique identifier for each currency record"}),
        StructField("code", StringType(), True, {"comment": "A three-letter currency code e.g. USD, EUR, CHF"}),
        StructField("name", StringType(), True, {"comment": "The official currency name in English e.g. US Dollar, Euro, Swiss Franc"}),
        StructField("source_created_at", StringType(), True, {"comment": "The timestamp when the record was originally created in the source system EBX"}),
        StructField("source_created_by", StringType(), True, {"comment": "The user who originally created the record in the source system EBX"}),
        StructField("source_updated_at", StringType(), True, {"comment": "The timestamp when the record was last updated in the source system EBX"}),
        StructField("source_updated_by", StringType(), True, {"comment": "The user who last updated the record in the source system EBX"}),
        StructField("last_published_cet", StringType(), True, {"comment": "The timestamp when the record was last published in the source system EBX"}),
        StructField("license", StringType(), True, {"comment": "The license package identifier (LPI) associated with the data"})
    ]),
    comment="List of all official currencies and its code worldwide",
    sources=[
        TableIdentifier(catalog="bronze", schema="ebx", name="currency")
    ],
    primary_keys=("pk_currency",),
    tags={
        **REFERENCE_COMMON_TAGS,
        standard_tags.DataOwner.KEY: standard_tags.DataOwner.MARIA_LOURDES_SANCHEZ_NUNEZ,
    }
)
