from pyspark.sql.types import (
    BooleanType,
    DateType,
    IntegerType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

from data_platform.data_model import StaticTableModel, TableIdentifier
from data_platform.data_model.metadata_common import standard_columns, standard_tags

reportsview = StaticTableModel(
    identifier=TableIdentifier(
        catalog="silver",
        schema="elviz_no",
        name="reports",
    ),
    schema=StructType(
        [
            StructField("report_id", IntegerType(), True, {"comment": "A unique identifier for the report"}),
            StructField("job_execution_id", IntegerType(), True, {"comment": "Identifier for the job run that generated the report"}),
            StructField("report_date", DateType(), True, {"comment": "Date of report"}),
            StructField("filter_name", StringType(), True, {"comment": "Name of the filter applied"}),
            StructField("intraday", BooleanType(), True, {"comment": "Flag that indicates whether the report is based on intraday data or not"}),
            StructField("use_actual_date_spot_rate", BooleanType(), True, {"comment": "Flag that indicates whether the report used actual spot rates"}),
            StructField("intrinsic_evaluation_of_capacity", BooleanType(), True, {"comment": "Flag that indicates whether intrinsic capacity valuation was applied"}),
            StructField("job_description", StringType(), True, {"comment": "Description of the job"}),
            StructField("completed_date_time", TimestampType(), True, {"comment": "Datetime when the report generation completed"}),
            StructField("status", StringType(), True, {"comment": "Status of the report generation process"}),
            StructField("is_latest_for_report_date", BooleanType(), True, {"comment": "Flag that indicates whether the report is latest for the report date"}),
            StructField("report_currency", StringType(), True, {"comment": "Currency used in the report"}),
            StructField("use_close_prices", BooleanType(), True, {"comment": "Flag that indicates whether closing prices were used in the report"}),
            standard_columns.LicenseColumn.to_struct_field(),
        ]
    ),
    comment="This table is a reproduction of the ReportsView from the Elviz Nordics database.",
    sources=(TableIdentifier(catalog="bronze", schema="elviz_no", name="reportsview"),),
    primary_keys=("report_id",),
    tags={
        standard_tags.ConfidentialityLevel.KEY: standard_tags.ConfidentialityLevel.C2_INTERNAL,
        standard_tags.DataOwner.KEY: standard_tags.DataOwner.PER_SIVERSEN,
        standard_tags.LicensedData.KEY: standard_tags.LicensedData.TRUE,
        standard_tags.PIIClassification.KEY: standard_tags.PIIClassification.NO_PII,
    },
)
