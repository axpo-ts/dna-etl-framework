from pyspark.sql.types import DoubleType, StringType, StructField, StructType

from data_platform.data_model import StaticTableModel, TableIdentifier
from data_platform.data_model.metadata_common import standard_columns, standard_tags

dayahead_volume_flexpool_table = StaticTableModel(
    identifier=TableIdentifier(catalog="silver", schema="advanced_analytics_platform", name="dayahead_volume_flexpool"),
    comment="The day-ahead closed volumes for flexpool Axpo France.",
    schema=StructType([
        standard_columns.DeliveryStartColumn.to_struct_field(nullable=True),
        standard_columns.DeliveryEndColumn.to_struct_field(nullable=True),
        standard_columns.DurationColumn.to_struct_field(),
        StructField("year_month_day", StringType(), True, {"comment": "The datetime when the file is created in UTC"}),
        StructField("run_time_id", StringType(), True, {"comment": "A 14-digit identifier representing date and time of run time in UTC in the format yyyymmddhhmmss e.g. 20241029100044"}),
        StructField("project_name_fr_actual_too", StringType(), True, {"comment": "The project name within the flexpool in France to which the value is associated"}),
        StructField("timeseries_id_ch_actual_too", StringType(), True, {"comment": "A textual identifier combining the data name and the unit e.g. day_ahead_sell_price_forecast_EURpMWh"}),
        StructField("value", DoubleType(), True, {"comment": "The value of the data point named in timeseries_id_ch_actual_too  and with unit indicated in the name"}),
        standard_columns.UnitColumn.to_struct_field(),
        standard_columns.CommodityColumn.to_struct_field(),
        standard_columns.LicenseColumn.to_struct_field(nullable=False),
        standard_columns.DataSourceColumn.to_struct_field(),
        standard_columns.DataSystemColumn.to_struct_field(),
        StructField("updated_at_source", StringType(), True, {"comment": "The datetime when the data are updated in UTC"})
    ]),
    sources=[TableIdentifier(catalog="bronze", schema="advanced_analytics_shared", name="fact_france_timeseries"), TableIdentifier(catalog="bronze", schema="advanced_analytics_shared", name="attribute")],
    tags={
        standard_tags.ConfidentialityLevel.KEY: standard_tags.ConfidentialityLevel.C2_INTERNAL,
        standard_tags.DataDomain.KEY: standard_tags.DataDomain.MARKET,
        standard_tags.DataSubdomain.KEY: standard_tags.DataSubdomain.SPOT,
        standard_tags.LicensedData.KEY: standard_tags.LicensedData.TRUE,
        standard_tags.PIIClassification.KEY: standard_tags.PIIClassification.NO_PII,
        standard_tags.DataOwner.KEY: standard_tags.DataOwner.GIANLUCA_MANCINI,
        },
    primary_keys=(),
    partition_cols=(),
    liquid_cluster_cols=(),
)
