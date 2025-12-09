from pyspark.sql.types import StringType, StructField, StructType

from data_platform.data_model import StaticTableModel, TableIdentifier
from data_platform.data_model.metadata_common import standard_tags
from data_platform.data_model.silver.reference.reference_common import REFERENCE_COMMON_TAGS

country_table = StaticTableModel(
    identifier=TableIdentifier(
        catalog="silver",
        schema="reference",
        name="country"
    ),
    schema=StructType([
        StructField("pk_country", StringType(), False, {"comment": "A unique identifier for each country record"}),
        StructField("alpha3_code", StringType(), True, {"comment": "A three-letter country code following ISO 3166-1 alpha-3 standard e.g. USA, DEU, CHE"}),
        StructField("alpha2_code", StringType(), True, {"comment": "A two-letter country code following ISO 3166-1 alpha-2 standard e.g. US, DE, CH"}),
        StructField("name", StringType(), True, {"comment": "The official country name in English e.g. United States of America, Germany, Switzerland"}),
        StructField("source_created_at", StringType(), True, {"comment": "The timestamp when the record was originally created in the source system EBX"}),
        StructField("source_created_by", StringType(), True, {"comment": "The user who originally created the record in the source system EBX"}),
        StructField("source_updated_at", StringType(), True, {"comment": "The timestamp when the record was last updated in the source system EBX"}),
        StructField("source_updated_by", StringType(), True, {"comment": "The user who last updated the record in the source system EBX"}),
        StructField("last_published_cet", StringType(), True, {"comment": "The timestamp when the record was last published in the source system EBX"}),
        StructField("license", StringType(), True, {"comment": "The license package identifier (LPI) associated with the data"})
    ]),
    comment=(
        "Country list according to ISO 3166-1. To differentiate with ISO 3166-2, "
        "the latter is part of the ISO 3166 standard published by the International "
        "Organization for Standardization (ISO), and defines codes for identifying "
        "the principal subdivisions (e.g., provinces or states) of all countries "
        "coded in ISO 3166-1"
    ),
    primary_keys=("pk_country",),
    tags={
        **REFERENCE_COMMON_TAGS,
        standard_tags.DataOwner.KEY: standard_tags.DataOwner.MARIA_LOURDES_SANCHEZ_NUNEZ,
    }
)
