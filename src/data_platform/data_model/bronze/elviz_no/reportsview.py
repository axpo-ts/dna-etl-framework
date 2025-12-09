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

reportsview = StaticTableModel(
    identifier=TableIdentifier(
        catalog="bronze",
        schema="elviz_no",
        name="reportsview",
    ),
    schema=StructType(
        [
            StructField("ReportId", IntegerType(), True),
            StructField("JobExecutionId", IntegerType(), True),
            StructField("ReportDate", DateType(), True),
            StructField("FilterName", StringType(), True),
            StructField("Intraday", BooleanType(), True),
            StructField("UseActualDateSpotRate", BooleanType(), True),
            StructField("IntrinsicEvaluationOfCapacity", BooleanType(), True),
            StructField("JobDescription", StringType(), True),
            StructField("CompletedDateTime", TimestampType(), True),
            StructField("Status", StringType(), True),
            StructField("IsLatestForReportDate", BooleanType(), True),
            StructField("ReportCurrency", StringType(), True),
            StructField("UseClosePrices", BooleanType(), True),
            StructField("license", StringType(), True),
        ]
    ),
    comment="This table is a reproduction of the ReportsView from the Elviz Nordics database.",
    sources=(TableIdentifier(catalog="axpots_elviz_nordics", schema="dbo", name="reportsview"),),
    primary_keys=("ReportId",),
    tags={
        "Confidentiality Level": "C2_Internal",
        "Personal sensitive information": "No PII",
        "Source System": "elviz_no",
        "Data Owner": "Per Siversen",
    },
)
