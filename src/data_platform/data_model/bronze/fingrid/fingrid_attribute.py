from __future__ import annotations

from pyspark.sql.types import ArrayType, IntegerType, StringType, StructField, StructType, TimestampType

from data_platform.data_model import FileVolumeIdentifier, StaticTableModel, TableIdentifier
from data_platform.data_model.metadata_common import standard_tags

fingrid_attribute_table = StaticTableModel(
    identifier=TableIdentifier(catalog="bronze", schema="fingrid", name="attribute"),
    schema=StructType(
        [
            # Attribute fields
            StructField("id", IntegerType(), True, {"comment": "Unique dataset identifier from Fingrid API"}),
            StructField(
                "modified_at_utc",
                TimestampType(),
                True,
                {"comment": "Timestamp when dataset attribute was last modified in UTC"},
            ),
            StructField(
                "data_available_from_utc",
                TimestampType(),
                True,
                {"comment": "Timestamp indicating when data becomes available from in UTC"},
            ),
            StructField("type", StringType(), True, {"comment": "Dataset type classification"}),
            StructField("status", StringType(), True, {"comment": "Current dataset status"}),
            StructField("organization", StringType(), True, {"comment": "Organization responsible for the dataset"}),
            StructField("name_en", StringType(), True, {"comment": "Dataset name in English"}),
            StructField("name_fi", StringType(), True, {"comment": "Dataset name in Finnish"}),
            StructField("description_en", StringType(), True, {"comment": "Detailed dataset description in English"}),
            StructField("description_fi", StringType(), True, {"comment": "Detailed dataset description in Finnish"}),
            StructField(
                "data_period_en",
                StringType(),
                True,
                {"comment": "Data collection period and granularity description in English"},
            ),
            StructField(
                "data_period_fi",
                StringType(),
                True,
                {"comment": "Data collection period and granularity description in Finnish"},
            ),
            StructField(
                "update_cadence_en",
                StringType(),
                True,
                {"comment": "How frequently the dataset is updated, described in English"},
            ),
            StructField(
                "update_cadence_fi",
                StringType(),
                True,
                {"comment": "How frequently the dataset is updated, described in Finnish"},
            ),
            StructField(
                "unit_en", StringType(), True, {"comment": "Unit of measurement for the data values in English"}
            ),
            StructField(
                "unit_fi", StringType(), True, {"comment": "Unit of measurement for the data values in Finnish"}
            ),
            StructField(
                "contact_persons",
                StringType(),
                True,
                {"comment": "Contact information for dataset maintainers or support"},
            ),
            StructField("license_name", StringType(), True, {"comment": "Name of the license governing dataset usage"}),
            StructField(
                "license_terms_link",
                StringType(),
                True,
                {"comment": "URL link to the full license terms and conditions"},
            ),
            StructField(
                "key_words_en",
                ArrayType(StringType()),
                True,
                {"comment": "Array of keywords describing the dataset in English"},
            ),
            StructField(
                "key_words_fi",
                ArrayType(StringType()),
                True,
                {"comment": "Array of keywords describing the dataset in Finnish"},
            ),
            StructField(
                "content_groups_en",
                ArrayType(StringType()),
                True,
                {"comment": "Array of content group classifications in English"},
            ),
            StructField(
                "content_groups_fi",
                ArrayType(StringType()),
                True,
                {"comment": "Array of content group classifications in Finnish"},
            ),
            StructField(
                "available_formats", ArrayType(StringType()), True, {"comment": "Array of available data formats"}
            ),
            # Technical metadata fields
            StructField("_rescued_data", StringType(), True, {"comment": "Rescued data column for schema evolution"}),
            StructField("file_path", StringType(), False, {"comment": "Source file path"}),
            # Audit columns will be added by CreateTable class
        ]
    ),
    comment="Flattened Fingrid attribute.",
    sources=[
        FileVolumeIdentifier(catalog="staging", schema="fingrid", name="attribute"),
    ],
    tags={
        standard_tags.ConfidentialityLevel.KEY: standard_tags.ConfidentialityLevel.C1_PUBLIC,
        standard_tags.DataDomain.KEY: standard_tags.DataDomain.FUNDAMENTAL,
        standard_tags.DataSubdomain.KEY: standard_tags.DataSubdomain.TRANSMISSION,
        standard_tags.LicensedData.KEY: standard_tags.LicensedData.TRUE,
        standard_tags.PIIClassification.KEY: standard_tags.PIIClassification.NO_PII,
        standard_tags.DataOwner.KEY: standard_tags.DataOwner.VALENTIN_JULIA,
    },
    primary_keys=("id", "modified_at_utc"),
)
