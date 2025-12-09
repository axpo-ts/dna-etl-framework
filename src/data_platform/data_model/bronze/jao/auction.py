from pyspark.sql.types import (
    ArrayType,
    BooleanType,
    DoubleType,
    IntegerType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

from data_platform.data_model import FileVolumeIdentifier, StaticTableModel, TableIdentifier

auction_table = StaticTableModel(
    identifier=TableIdentifier("bronze", "jao", "auction"),
    schema=StructType(
        [
            StructField("corridorCode", StringType(), True),
            StructField("productHour", StringType(), True),
            StructField("marketPeriodStart", TimestampType(), True),
            StructField("marketPeriodStop", TimestampType(), True),
            StructField("ftroption", StringType(), True),
            StructField("identification", StringType(), True),
            StructField("horizonName", StringType(), True),
            StructField("bidGateOpening", TimestampType(), True),
            StructField("bidGateClosure", TimestampType(), True),
            StructField("isBidGateOpen", BooleanType(), True),
            StructField("atcGateOpening", TimestampType(), True),
            StructField("atcGateClosure", TimestampType(), True),
            StructField("disputeSubmissionGateOpening", TimestampType(), True),
            StructField("disputeSubmissionGateClosure", TimestampType(), True),
            StructField("disputeProcessGateOpening", TimestampType(), True),
            StructField("disputeProcessGateClosure", TimestampType(), True),
            StructField("ltResaleGateOpening", TimestampType(), True),
            StructField("ltResaleGateClosure", TimestampType(), True),
            StructField("periodToBeSecuredStart", TimestampType(), True),
            StructField("periodToBeSecuredStop", TimestampType(), True),
            StructField("xnRule", StringType(), True),
            StructField("lastDataUpdate", TimestampType(), True),
            StructField("cancelled", BooleanType(), True),
            StructField(
                "maintenances",
                ArrayType(
                    StructType(
                        [
                            StructField("corridorCode", StringType(), True),
                            StructField("offeredCapacity", DoubleType(), True),
                            StructField("atc", DoubleType(), True),
                            StructField("periodStart", TimestampType(), True),
                            StructField("periodStop", TimestampType(), True),
                            StructField("deliveryMinutes", DoubleType(), True),
                            StructField("reducedOfferedCapacity", DoubleType(), True),
                        ]
                    )
                ),
                True,
            ),
            StructField("productComment", StringType(), True),
            StructField("resultComment", StringType(), True),
            StructField("winningParties", ArrayType(StringType()), True),
            StructField("resultOfferedCapacity", DoubleType(), True),
            StructField("resultRequestedCapacity", DoubleType(), True),
            StructField("resultAllocatedCapacity", DoubleType(), True),
            StructField("resultAuctionPrice", DoubleType(), True),
            StructField("resultAdditionalMessage", StringType(), True),
            StructField("operationalMessage", StringType(), True),
            StructField("productIdentification", StringType(), True),
            StructField("productBidderPartyCount", IntegerType(), True),
            StructField("productATC", DoubleType(), True),
            StructField("productResoldCapacity", DoubleType(), True),
            StructField("productOfferedCapacity", DoubleType(), True),
            StructField("productWinnerPartyCount", IntegerType(), True),
        ]
    ),
    sources=[FileVolumeIdentifier(catalog="staging", schema="jao", name="auction")],
    primary_keys=(),
)
