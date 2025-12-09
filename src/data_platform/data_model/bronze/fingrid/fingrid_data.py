from __future__ import annotations

from pyspark.sql.types import FloatType, LongType, StringType, StructField, StructType, TimestampType

from data_platform.data_model import FileVolumeIdentifier, StaticTableModel, TableIdentifier
from data_platform.data_model.metadata_common import standard_tags

fingrid_data_table = StaticTableModel(
    identifier=TableIdentifier(catalog="bronze", schema="fingrid", name="data"),
    schema=StructType(
        [
            # Data fields
            StructField("dataset_id", LongType(), True, {"comment": "Dataset identifier from data array"}),
            StructField("start_time", TimestampType(), True, {"comment": "Start time from data array"}),
            StructField("end_time", TimestampType(), True, {"comment": "End time from data array"}),
            StructField("value", FloatType(), True, {"comment": "Data value from data array"}),
            # Pagination fields
            StructField("pagination_total", LongType(), True, {"comment": "Total number of records"}),
            StructField("pagination_last_page", LongType(), True, {"comment": "Last page number"}),
            StructField("pagination_prev_page", LongType(), True, {"comment": "Previous page number"}),
            StructField("pagination_next_page", LongType(), True, {"comment": "Next page number"}),
            StructField("pagination_per_page", LongType(), True, {"comment": "Records per page"}),
            StructField("pagination_current_page", LongType(), True, {"comment": "Current page number"}),
            StructField("pagination_from", LongType(), True, {"comment": "Starting record number"}),
            StructField("pagination_to", LongType(), True, {"comment": "Ending record number"}),
            # Technical metadata fields
            StructField("_rescued_data", StringType(), True, {"comment": "Rescued data column for schema evolution"}),
            StructField("file_path", StringType(), False, {"comment": "Source file path"}),
            # Audit columns will be added by CreateTable class
        ]
    ),
    comment="Flattened Fingrid data.",
    sources=[
        FileVolumeIdentifier(catalog="staging", schema="fingrid", name="data"),
    ],
    tags={
        standard_tags.ConfidentialityLevel.KEY: standard_tags.ConfidentialityLevel.C1_PUBLIC,
        standard_tags.DataDomain.KEY: standard_tags.DataDomain.FUNDAMENTAL,
        standard_tags.DataSubdomain.KEY: standard_tags.DataSubdomain.TRANSMISSION,
        standard_tags.LicensedData.KEY: standard_tags.LicensedData.TRUE,
        standard_tags.PIIClassification.KEY: standard_tags.PIIClassification.NO_PII,
        standard_tags.DataOwner.KEY: standard_tags.DataOwner.VALENTIN_JULIA,
    },
    primary_keys=("dataset_id", "start_time"),
)
