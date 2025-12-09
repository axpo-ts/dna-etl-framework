from __future__ import annotations

from pyspark.sql.types import (
    IntegerType,
    StringType,
    StructField,
    StructType,
)

from data_platform.data_model import StaticTableModel, TableIdentifier
from data_platform.data_model.metadata_common import standard_columns, standard_tags
from data_platform.data_model.silver.reference.reference_common import (
    REFERENCE_COMMON_TAGS,
    REFERENCE_LICENSE,
)

data_system_reference_table = StaticTableModel(
    identifier=TableIdentifier(
        catalog="silver",
        schema="reference",
        name="data_system",
    ),
    schema=StructType(
        [
            # Core identification
            StructField("id", IntegerType(), False, {"comment": "Unique identifier for the data system"}),
            StructField("internal_system_name", StringType(), True, {"comment": "Original system identifier/name"}),
            StructField("description", StringType(), True, {"comment": "Detailed description of the data system"}),
            StructField("source_type", StringType(), True, {"comment": "Indicates whether the data system is internal to the organization or external"}),
            StructField("status", StringType(), True, {"comment": "Current status of the data system (active, inactive, deprecated)"}),
            StructField("tds", StringType(), True, {"comment": "Transactional Data Store reference"}),
            StructField("link", StringType(), True, {"comment": "URL or reference link to system documentation"}),
            # Audit and validity
            standard_columns.ValidFromColumn.to_struct_field(nullable=False, comment="Start date of system validity"),
            standard_columns.ValidToColumn.to_struct_field(comment="End date of system validity (NULL for current)"),
            standard_columns.IsCurrentColumn.to_struct_field(nullable=False, comment="Flag indicating if this is the current active system record"),
        ]
    ),
    comment=(
        "Internal name for the system/application/teams from which the data is ingested into DnAP / TDS "
        "(direct interface). Alias/Synonym: data interface, application, api. "
        "Currently used to organise bronze layer in DnAP & TDS and silver layer in DnAP. "
        "This is not a data source, but rather a system that provides data to DnAP / TDS."
    ),
    primary_keys=("id", "valid_from"),
    license=REFERENCE_LICENSE,
    tags={
        **REFERENCE_COMMON_TAGS,
        standard_tags.DataOwner.KEY: standard_tags.DataOwner.NORBERT_DORNDORF,
    },
    audit_columns=("created_by", "updated_by"),
)

data_source_reference_table = StaticTableModel(
    identifier=TableIdentifier(
        catalog="silver",
        schema="reference",
        name="data_source",
    ),
    schema=StructType(
        [
            # Core identification
            StructField("id", IntegerType(), False, {"comment": "Unique identifier for the data source"}),
            StructField("source_name_original", StringType(), True, {"comment": "Original name of the data source as provided externally"}),
            StructField("internal_source_name", StringType(), True, {"comment": "Internal standardized name for the data source"}),
            StructField("description", StringType(), True, {"comment": "Detailed description of the data source"}),
            StructField("internal_external", StringType(), True, {"comment": "Classification of whether the source is internal or external"}),
            StructField("status", StringType(), True, {"comment": "Current status of the data source (active, inactive, proposed, todo)"}),
            StructField("tds", StringType(), True, {"comment": "Transactional Data Store reference"}),
            StructField("data_system", StringType(), True, {"comment": "Associated data system for this source"}),
            # Audit and validity
            standard_columns.ValidFromColumn.to_struct_field(nullable=False, comment="Start date of source validity"),
            standard_columns.ValidToColumn.to_struct_field(comment="End date of source validity (NULL for current)"),
            standard_columns.IsCurrentColumn.to_struct_field(nullable=False, comment="Flag indicating if this is the current active source record"),
        ]
    ),
    comment=(
        "Reference table for data sources that provide data to the platform. "
        "This includes both internal and external data sources, APIs, databases, "
        "and other data providers that feed into the data platform."
    ),
    primary_keys=("id", "valid_from"),
    license=REFERENCE_LICENSE,
    tags={
        **REFERENCE_COMMON_TAGS,
        standard_tags.DataOwner.KEY: standard_tags.DataOwner.NORBERT_DORNDORF,
    },
    audit_columns=("created_by", "updated_by"),
)
