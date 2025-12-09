from pyspark.sql.types import DateType, StringType, StructField, StructType

from data_platform.data_model import StaticTableModel, TableIdentifier
from data_platform.data_model.metadata_common import standard_columns, standard_tags

contract_table = StaticTableModel(
    identifier=TableIdentifier(catalog="silver", schema="ckw", name="contract"),
    schema=StructType([
        standard_columns.LicenseColumn.to_struct_field(),
        StructField("plant_number", StringType(), True, {"comment": "Identifier for the plant"}),
        StructField("service_provider_id", StringType(), True, {"comment": "Identifier for the service provider"}),
        StructField("service_provider_name", StringType(), True, {"comment": "Name of the service provider"}),
        StructField("service_type_id", StringType(), True, {"comment": "Identifier for the type of service"}),
        StructField("service_type_name", StringType(), True, {"comment": "Name of the type of service"}),
        StructField("extract_date", DateType(), True, {"comment": "Date when the data was extracted  to the ckw database"}),
        standard_columns.DataSystemColumn.to_struct_field(),
        standard_columns.DataSourceColumn.to_struct_field(),
    ]),
    comment="Contains information about contracts, including details such as license, plant number, service provider, service type, and timestamps for record creation and updates. It is used to track and manage contract-related data within the CKW installations.",
    sources=[
        TableIdentifier(catalog="bronze", schema="ckw", name="tbl_d_isu_vertrag_filtered"),
        TableIdentifier(catalog="bronze", schema="ckw", name="attribute"),
    ],
    tags={
        standard_tags.ConfidentialityLevel.KEY: standard_tags.ConfidentialityLevel.C2_INTERNAL,
        standard_tags.DataDomain.KEY: standard_tags.DataDomain.COUNTERPARTY,
        standard_tags.LicensedData.KEY: standard_tags.LicensedData.TRUE,
        standard_tags.PIIClassification.KEY: standard_tags.PIIClassification.NO_PII,
        standard_tags.DataOwner.KEY: standard_tags.DataOwner.REMI_JANNER,
    },
    primary_keys=(),
    partition_cols=(),
    liquid_cluster_cols=(),
)
