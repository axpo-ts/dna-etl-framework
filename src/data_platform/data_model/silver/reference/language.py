from pyspark.sql.types import StringType, StructField, StructType

from data_platform.data_model import StaticTableModel, TableIdentifier
from data_platform.data_model.metadata_common import standard_tags
from data_platform.data_model.silver.reference.reference_common import REFERENCE_COMMON_TAGS

language_table = StaticTableModel(
    identifier=TableIdentifier(
        catalog="silver",
        schema="reference",
        name="language"
    ),
    schema=StructType([
        StructField("pk_language", StringType(), False, {"comment": "A unique identifier for each language record"}),
        StructField("code", StringType(), True, {"comment": "A two-letter language code following ISO 639-1 standard e.g. en, de, fr"}),
        StructField("name", StringType(), True, {"comment": "The official language name in English e.g. English, German, French"}),
        StructField("source_created_at", StringType(), True, {"comment": "The timestamp when the record was originally created in the source system EBX"}),
        StructField("source_created_by", StringType(), True, {"comment": "The user who originally created the record in the source system EBX"}),
        StructField("source_updated_at", StringType(), True, {"comment": "The timestamp when the record was last updated in the source system EBX"}),
        StructField("source_updated_by", StringType(), True, {"comment": "The user who last updated the record in the source system EBX"}),
        StructField("last_published_cet", StringType(), True, {"comment": "The timestamp when the record was last published in the source system EBX"}),
        StructField("license", StringType(), True, {"comment": "The license package identifier (LPI) associated with the data"})
    ]),
    comment=(
        "Language list according to ISO 639-1 (ISO 639-1 is the alpha-2 code and it is a subset of "
        "ISO 639-2 that is the alpha-3 code). ISO 639-1 includes the most common and relevant languages "
        "in everyday use and is the most widely used in technological systems. ISO 639-2 includes regional "
        "languages among others. Since it has only two letters, ISO 639-1 codes are more practical and "
        "easier to implement and identify."
    ),
    sources=[
        TableIdentifier(catalog="bronze", schema="ebx", name="language")
    ],
    primary_keys=("pk_language",),
    tags={
        **REFERENCE_COMMON_TAGS,
        standard_tags.DataOwner.KEY: standard_tags.DataOwner.MARIA_LOURDES_SANCHEZ_NUNEZ,
    }
)
