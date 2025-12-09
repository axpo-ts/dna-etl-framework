from pyspark.sql.types import DateType, DoubleType, IntegerType, StringType, StructField, StructType, TimestampType

from data_platform.data_model import StaticTableModel, TableIdentifier

timeseriesbycontractsview = StaticTableModel(
    identifier=TableIdentifier(
        catalog="bronze",
        schema="elviz_no",
        name="timeseriesbycontractsview",
    ),
    schema=StructType(
        [
            StructField("ReportId", IntegerType(), True),
            StructField("ContractExportId", IntegerType(), True),
            StructField("TimeSeriesAlias", StringType(), True),
            StructField("FromDateTime", TimestampType(), True),
            StructField("FromDateTimeUTC", TimestampType(), True),
            StructField("Resolution", StringType(), True),
            StructField("TimeSeriesTimeZone", StringType(), True),
            StructField("LoadType", StringType(), True),
            StructField("LoadHours", DoubleType(), True),
            StructField("ValueType", StringType(), True),
            StructField("ValueUnit", StringType(), True),
            StructField("Value", DoubleType(), True),
            StructField("FeeType", StringType(), True),
            StructField("PriceBasisName", StringType(), True),
            StructField("ReportDate", DateType(), True),
            StructField("license", StringType(), True),
        ]
    ),
    comment="This table is a reproduction of the TimeSeriesContractsView from the Elviz Nordics database.",
    sources=(TableIdentifier(catalog="axpots_elviz_nordics", schema="dbo", name="timeseriesbycontractsview"),),
    primary_keys=(),
    tags={
        "Confidentiality Level": "C2_Internal",
        "Personal sensitive information": "No PII",
        "Source System": "elviz_no",
        "Data Owner": "Per Siversen",
    },
)
