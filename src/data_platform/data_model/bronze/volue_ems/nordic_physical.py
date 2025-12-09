from pyspark.sql.types import BooleanType, DoubleType, StringType, StructField, StructType, TimestampType

from data_platform.data_model import StaticTableModel, TableIdentifier

nordic_physical_table = StaticTableModel(
    identifier=TableIdentifier("bronze", "volue_ems", "nordic_physical"),
    schema=StructType(
        [
            StructField("DataGroup", StringType(), True),
            StructField("Name", StringType(), True),
            StructField("IsWriteAllowed", BooleanType(), True),
            StructField("Unit", StringType(), True),
            StructField("TimeLevel", StringType(), True),
            StructField("TimeStamp", TimestampType(), True),
            StructField("Value", DoubleType(), True),
            StructField("Status", StringType(), True),
            StructField("Updated", StringType(), True),
            StructField("UpdatedBy", StringType(), True),
        ]
    ),
)
