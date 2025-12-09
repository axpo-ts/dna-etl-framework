from pyspark.sql.types import LongType, StringType, StructField, StructType

from data_platform.data_model import StaticTableModel, TableIdentifier
from data_platform.data_model.metadata_common import standard_columns, standard_tags

customer_group_table = StaticTableModel(
    identifier=TableIdentifier(
        catalog="silver",
        schema="ebx",
        name="customer_group"
    ),
    schema=StructType([
        StructField("customer_group_id", LongType(), False, {"comment": ""}),
        StructField("customer_group_name", StringType(), True, {"comment": ""}),
        standard_columns.DataSystemColumn.to_struct_field(),
        standard_columns.DataSourceColumn.to_struct_field()
    ]),
    tags={
        standard_tags.ConfidentialityLevel.KEY: standard_tags.ConfidentialityLevel.C2_INTERNAL,
        standard_tags.PIIClassification.KEY: standard_tags.PIIClassification.NO_PII,
        standard_tags.DataDomain.KEY: standard_tags.DataDomain.COUNTERPARTY,
        standard_tags.LicensedData.KEY: standard_tags.LicensedData.FALSE
    },
)
