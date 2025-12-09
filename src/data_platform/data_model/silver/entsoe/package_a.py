from __future__ import annotations

from pyspark.sql.types import (
    DoubleType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

from data_platform.data_model import StaticTableModel, TableIdentifier
from data_platform.data_model.metadata_common import standard_columns, standard_tags
from data_platform.data_model.silver.entsoe.entsoe_common import ENTSOE_COMMON_TAGS, entsoe_standard_columns

# Physical Flow Cross Border Table
physical_flow_cross_border_table = StaticTableModel(
    identifier=TableIdentifier(catalog="silver", schema="entsoe", name="physical_flow_cross_border"),
    schema=StructType(
        [
            standard_columns.DeliveryStartColumn.to_struct_field(),
            standard_columns.DeliveryEndColumn.to_struct_field(),
            standard_columns.DurationColumn.to_struct_field(),
            StructField("out_area_code", StringType(), False, {"comment": "The area code where power is flowing out of"}),
            StructField("out_area_type_code", StringType(), True, {"comment": "The type of the area where power is flowing out of"}),
            StructField("out_area_name", StringType(), True, {"comment": "The area name where power is flowing out of"}),
            StructField("out_area_map_code", StringType(), True, {"comment": "The map code where power is flowing out of"}),
            StructField("in_area_code", StringType(), False, {"comment": "The area code where power is flowing into"}),
            StructField("in_area_type_code", StringType(), True, {"comment": "The type of the area where power is flowing in"}),
            StructField("in_area_name", StringType(), True, {"comment": "The area name where power is flowing in"}),
            StructField("in_area_map_code", StringType(), True, {"comment": "The map code where power is flowing in"}),
            StructField("flow_value", DoubleType(), True, {"comment": "The physical flows between bidding zones per market time"}),
            standard_columns.UnitColumn.to_struct_field(),
            standard_columns.CommodityColumn.to_struct_field(),
            standard_columns.LicenseColumn.to_struct_field(),
            standard_columns.DataSourceColumn.to_struct_field(),
            standard_columns.DataSystemColumn.to_struct_field(),
            standard_columns.UpdatedAtSourceColumn.to_struct_field(),
        ]
    ),
    comment="This table contains records of actual physical electricity flows between bidding zones or countries across cross-border interconnectors, supporting transparency and analysis of real-time cross-border electricity movements. The data is sourced from ENTSO-E Physical Flows (12.1.G).",
    sources=[
        TableIdentifier(catalog="bronze", schema="entsoe", name="physicalflows_12_1_g_r3"),
    ],
    tags={**ENTSOE_COMMON_TAGS, standard_tags.DataSubdomain.KEY: standard_tags.DataSubdomain.TRANSMISSION},
    primary_keys=("delivery_start", "delivery_end", "out_area_code", "out_area_type_code", "in_area_code", "updated_at_source"),
)

# Transfer Capacity Net Day-ahead Forecast Table
transfer_capacity_net_dayahead_forecast_table = StaticTableModel(
    identifier=TableIdentifier(catalog="silver", schema="entsoe", name="transfer_capacity_net_dayahead_forecast"),
    schema=StructType(
        [
            standard_columns.DeliveryStartColumn.to_struct_field(),
            standard_columns.DeliveryEndColumn.to_struct_field(),
            standard_columns.DurationColumn.to_struct_field(),
            StructField("out_area_code", StringType(), False, {"comment": "The area code where power is flowing out of"}),
            StructField("out_area_type_code", StringType(), True, {"comment": "The type of the area where power is flowing out of"}),
            StructField("out_area_name", StringType(), True, {"comment": "The area name where power is flowing out of"}),
            StructField("out_area_map_code", StringType(), True, {"comment": "The map code where power is flowing out of"}),
            StructField("in_area_code", StringType(), False, {"comment": "The area code where power is flowing into"}),
            StructField("in_area_type_code", StringType(), True, {"comment": "The type of the area where power is flowing in"}),
            StructField("in_area_name", StringType(), True, {"comment": "The area name where power is flowing in"}),
            StructField("in_area_map_code", StringType(), True, {"comment": "The map code where power is flowing in"}),
            StructField("forecast_transfer_capacity", DoubleType(), True, {"comment": "The forecasted net transfer capacity between bidding zones, in the specified unit"}),
            standard_columns.UnitColumn.to_struct_field(),
            standard_columns.CommodityColumn.to_struct_field(),
            standard_columns.LicenseColumn.to_struct_field(),
            standard_columns.DataSourceColumn.to_struct_field(),
            standard_columns.DataSystemColumn.to_struct_field(),
            standard_columns.UpdatedAtSourceColumn.to_struct_field(),
        ]
    ),
    comment="This table contains day-ahead forecasts of net transfer capacity (NTC) for cross-border interconnections by border and direction. Each record provides the forecasted NTC value for a specific time interval, supporting market planning, capacity calculation transparency, and cross-border trading analysis. The data is sourced from ENTSO-E Forecasted Day-ahead Transfer Capacities (11.1)",
    sources=[
        TableIdentifier(catalog="bronze", schema="entsoe", name="forecastedtransfercapacities_11_1_r3"),
    ],
    tags={**ENTSOE_COMMON_TAGS, standard_tags.DataSubdomain.KEY: standard_tags.DataSubdomain.TRANSMISSION},
    primary_keys=("delivery_start", "delivery_end", "out_area_code", "out_area_type_code", "in_area_code", "updated_at_source"),
)

# Commercial Exchange Scheduled Table
commercial_exchange_scheduled_table = StaticTableModel(
    identifier=TableIdentifier(catalog="silver", schema="entsoe", name="commercial_exchange_scheduled"),
    schema=StructType(
        [
            standard_columns.DeliveryStartColumn.to_struct_field(),
            standard_columns.DeliveryEndColumn.to_struct_field(),
            standard_columns.DurationColumn.to_struct_field(),
            StructField("out_area_code", StringType(), False, {"comment": "The area code where power is flowing out of"}),
            StructField("out_area_display_name", StringType(), True, {"comment": "The area name where power is flowing out of"}),
            StructField("out_area_type_code", StringType(), True, {"comment": "The type of the area where power is flowing out of"}),
            StructField("out_area_map_code", StringType(), True, {"comment": "The map code where power is flowing out of"}),
            StructField("in_area_code", StringType(), False, {"comment": "The area code where power is flowing into"}),
            StructField("in_area_display_name", StringType(), True, {"comment": "The area name where power is flowing in"}),
            StructField("in_area_type_code", StringType(), True, {"comment": "The type of the area where power is flowing in"}),
            StructField("in_area_map_code", StringType(), True, {"comment": "The map code where power is flowing in"}),
            StructField("day_ahead_capacity", DoubleType(), True, {"comment": "The day-ahead scheduled commercial exchanges in the specified unit"}),
            StructField("total_capacity", DoubleType(), True, {"comment": "The total scheduled commercial exchanges in the specified unit"}),
            standard_columns.UnitColumn.to_struct_field(),
            standard_columns.CommodityColumn.to_struct_field(),
            standard_columns.LicenseColumn.to_struct_field(),
            standard_columns.DataSourceColumn.to_struct_field(),
            standard_columns.DataSystemColumn.to_struct_field(),
            standard_columns.UpdatedAtSourceColumn.to_struct_field(),
        ]
    ),
    comment="This table contains records of scheduled commercial electricity exchanges between different bidding zones or countries. Each entry provides details of the day-ahead and total capacity in the specified delivery period. The table supports transparency and analysis of cross-border electricity flows by offering insight into scheduled commercial transactions. The data is sourced from ENTSO-E Scheduled Commercial Exchanges (12.1.F).",
    sources=[
        TableIdentifier(catalog="bronze", schema="entsoe", name="commercialschedules_12_1_f_r3"),
    ],
    tags={**ENTSOE_COMMON_TAGS, standard_tags.DataSubdomain.KEY: standard_tags.DataSubdomain.TRANSMISSION},
    primary_keys=("delivery_start", "delivery_end", "out_area_code", "in_area_code", "updated_at_source"),
)

# Cost Congestion Management Table
cost_congestion_management_table = StaticTableModel(
    identifier=TableIdentifier(catalog="silver", schema="entsoe", name="cost_congestion_management"),
    schema=StructType(
        [
            StructField("year", StringType(), True, {"comment": "Year of the record"}),
            StructField("month", StringType(), True, {"comment": "Month of the record"}),
            StructField("time_zone", StringType(), True, {"comment": "Time zone"}),
            standard_columns.DurationColumn.to_struct_field(),
            entsoe_standard_columns.AreaCodeColumn.to_struct_field(nullable=False),
            StructField("area_display_name", StringType(), True, {"comment": "The name of the geographic area, market zone, or control area relevant to the data record"}),
            entsoe_standard_columns.AreaTypeCodeColumn.to_struct_field(),
            entsoe_standard_columns.AreaMapCodeColumn.to_struct_field(),
            StructField("redispatching_cost", DoubleType(), True, {"comment": "The portion of the total congestion management contributed to redispatching"}),
            StructField("countertrading_cost", DoubleType(), True, {"comment": "The portion of the total congestion management contributed to countertrading"}),
            StructField("other_cost", DoubleType(), True, {"comment": "Other costs of congestion management"}),
            StructField("total_cost", DoubleType(), True, {"comment": "Total cost of congestion management"}),
            standard_columns.CurrencyColumn.to_struct_field(),
            standard_columns.CommodityColumn.to_struct_field(),
            standard_columns.LicenseColumn.to_struct_field(),
            standard_columns.DataSourceColumn.to_struct_field(),
            standard_columns.DataSystemColumn.to_struct_field(),
            standard_columns.UpdatedAtSourceColumn.to_struct_field(),
        ]
    ),
    comment="This table contains records of the costs incurred for congestion management measures implemented by transmission system operators (TSOs) within contral areas. Each entry details the total cost, redispatching cost, and countertrading cost in the delivery period, supporting transparency and analysis of congestion management expenditures in the electricity market. The data is sourced from ENTSO-E Costs of Congestion Management (13.1.C).",
    sources=[
        TableIdentifier(catalog="bronze", schema="entsoe", name="costsofcongestionmanagement_13_1_c_r3"),
    ],
    tags={**ENTSOE_COMMON_TAGS, standard_tags.DataSubdomain.KEY: standard_tags.DataSubdomain.CONGESTION_MANAGEMENT},
    primary_keys=("year", "month", "area_code", "updated_at_source"),
)

# Cross Border Capacity DC Link Intraday Table
cross_border_capacity_dc_link_intraday_table = StaticTableModel(
    identifier=TableIdentifier(catalog="silver", schema="entsoe", name="cross_border_capacity_dc_link_intraday"),
    schema=StructType(
        [
            StructField("instance_code", StringType(), True, {"comment": "A unique code identifying a particular instance"}),
            standard_columns.DeliveryStartColumn.to_struct_field(),
            standard_columns.DeliveryEndColumn.to_struct_field(),
            standard_columns.DurationColumn.to_struct_field(),
            StructField("out_area_code", StringType(), False, {"comment": "The area code where power is flowing out of"}),
            StructField("out_area_type_code", StringType(), True, {"comment": "The type of the area where power is flowing out of"}),
            StructField("out_area_name", StringType(), True, {"comment": "The area name where power is flowing out of"}),
            StructField("out_area_map_code", StringType(), True, {"comment": "The map code where power is flowing out of"}),
            StructField("in_area_code", StringType(), False, {"comment": "The area code where power is flowing into"}),
            StructField("in_area_type_code", StringType(), True, {"comment": "The type of the area where power is flowing in"}),
            StructField("in_area_name", StringType(), True, {"comment": "The area name where power is flowing in"}),
            StructField("in_area_map_code", StringType(), True, {"comment": "The map code where power is flowing in"}),
            StructField("capacity_limit", DoubleType(), True, {"comment": "The intraday transfer capacity limit in the specified unit"}),
            standard_columns.UnitColumn.to_struct_field(),
            standard_columns.CommodityColumn.to_struct_field(),
            standard_columns.LicenseColumn.to_struct_field(),
            standard_columns.DataSourceColumn.to_struct_field(),
            standard_columns.DataSystemColumn.to_struct_field(),
            standard_columns.UpdatedAtSourceColumn.to_struct_field(),
        ]
    ),
    comment="This table contains records of available cross-border transmission capacity for direct current (DC) interconnectors during the intraday market timeframe. Each entry details the capacity limit, relevant bidding zone or control area, and the delivery period. The table supports transparency and analysis of DC link utilization and cross-border electricity trading opportunities. The data is sourced from ENTSO-E Cross-border Capacity for DC Links (11.3).",
    sources=[
        TableIdentifier(catalog="bronze", schema="entsoe", name="crossbordercapacityfordclinksintradaytransferlimits_11_3_r3"),
    ],
    tags={**ENTSOE_COMMON_TAGS, standard_tags.DataSubdomain.KEY: standard_tags.DataSubdomain.TRANSMISSION},
    primary_keys=("delivery_start", "delivery_end", "out_area_code", "out_area_type_code", "in_area_code", "in_area_type_code", "updated_at_source"),
)

# Redispatching Cross Border Table
redispatching_cross_border_table = StaticTableModel(
    identifier=TableIdentifier(catalog="silver", schema="entsoe", name="redispatching_cross_border"),
    schema=StructType(
        [
            StructField("instance_code", StringType(), True, {"comment": "A unique code identifying a particular instance"}),
            StructField("redispatching_start", TimestampType(), False, {"comment": "Start of redispatching period"}),
            StructField("redispatching_end", TimestampType(), False, {"comment": "End of redispatching period"}),
            StructField("time_series_start", TimestampType(), True, {"comment": "Start of time series"}),
            StructField("time_series_end", TimestampType(), True, {"comment": "End of time series"}),
            standard_columns.DurationColumn.to_struct_field(),
            StructField("out_area_code", StringType(), False, {"comment": "The area code where power is flowing out of"}),
            StructField("out_area_display_name", StringType(), True, {"comment": "The area name where power is flowing out of"}),
            StructField("out_area_type_code", StringType(), True, {"comment": "The type of the area where power is flowing out of"}),
            StructField("out_area_map_code", StringType(), True, {"comment": "The map code where power is flowing out of"}),
            StructField("in_area_code", StringType(), False, {"comment": "The area code where power is flowing into"}),
            StructField("in_area_display_name", StringType(), True, {"comment": "The area name where power is flowing in"}),
            StructField("in_area_type_code", StringType(), True, {"comment": "The type of the area where power is flowing in"}),
            StructField("in_area_map_code", StringType(), True, {"comment": "The map code where power is flowing in"}),
            StructField("affected_asset_code", StringType(), True, {"comment": "Affected asset code"}),
            StructField("affected_asset_name", StringType(), True, {"comment": "Affected asset name"}),
            StructField("affected_asset_type", StringType(), True, {"comment": "Affected asset type"}),
            StructField("affected_asset_location", StringType(), True, {"comment": "Affected asset location"}),
            StructField("connecting_market_participant_code", StringType(), True, {"comment": "Connecting Market Participant Code"}),
            StructField("connecting_market_participant_name", StringType(), True, {"comment": "Connecting Market Participant Name"}),
            StructField("capacity_impact", DoubleType(), True, {"comment": "Capacity impact"}),
            standard_columns.UnitColumn.to_struct_field(),
            StructField("action_type", StringType(), True, {"comment": "Action type"}),
            StructField("action_direction", StringType(), True, {"comment": "Action direction"}),
            StructField("reason", StringType(), True, {"comment": "Reason for redispatching"}),
            StructField("comment", StringType(), True, {"comment": "Additional comments"}),
            standard_columns.CommodityColumn.to_struct_field(),
            standard_columns.LicenseColumn.to_struct_field(),
            standard_columns.DataSourceColumn.to_struct_field(),
            standard_columns.DataSystemColumn.to_struct_field(),
            standard_columns.UpdatedAtSourceColumn.to_struct_field(),
        ]
    ),
    comment="This table records cross-border redispatching actions initiated by TSOs to relieve network congestions affecting flows between bidding zones or control areas. Each record includes the direction and magnitude of redispatching, affected areas, time interval, and relevant operational context, enabling transparency and analysis of congestion management and its cross-border impacts. The data is sourced from ENTSO-E Redispatching (13.1.A)",
    sources=[
        TableIdentifier(catalog="bronze", schema="entsoe", name="redispatchingcrossborder_13_1_a_r3_1"),
    ],
    tags={**ENTSOE_COMMON_TAGS, standard_tags.DataSubdomain.KEY: standard_tags.DataSubdomain.CONGESTION_MANAGEMENT},
    primary_keys=("redispatching_start", "redispatching_end", "time_series_start", "time_series_end", "out_area_code", "in_area_code", "updated_at_source"),
)

# Nominated Total Capacity Table
transfer_capacity_nominated_table = StaticTableModel(
    identifier=TableIdentifier(catalog="silver", schema="entsoe", name="nominated_total_capacity"),
    schema=StructType(
        [
            standard_columns.DeliveryStartColumn.to_struct_field(),
            standard_columns.DeliveryEndColumn.to_struct_field(),
            standard_columns.DurationColumn.to_struct_field(),
            StructField("out_area_code", StringType(), False, {"comment": "The area code where power is flowing out of"}),
            StructField("out_area_type_code", StringType(), True, {"comment": "The type of the area where power is flowing out of"}),
            StructField("out_area_name", StringType(), True, {"comment": "The area name where power is flowing out of"}),
            StructField("out_area_map_code", StringType(), True, {"comment": "The map code where power is flowing out of"}),
            StructField("in_area_code", StringType(), False, {"comment": "The area code where power is flowing into"}),
            StructField("in_area_type_code", StringType(), True, {"comment": "The type of the area where power is flowing in"}),
            StructField("in_area_name", StringType(), True, {"comment": "The area name where power is flowing in"}),
            StructField("in_area_map_code", StringType(), True, {"comment": "The map code where power is flowing in"}),
            StructField("contract_type", StringType(), True, {"comment": "The type of the contract, e.g. Day-ahead"}),
            StructField("capacity", DoubleType(), True, {"comment": "The nominated total capacity in the specified unit"}),
            standard_columns.UnitColumn.to_struct_field(),
            standard_columns.CommodityColumn.to_struct_field(),
            standard_columns.LicenseColumn.to_struct_field(),
            standard_columns.DataSourceColumn.to_struct_field(),
            standard_columns.DataSystemColumn.to_struct_field(),
            standard_columns.UpdatedAtSourceColumn.to_struct_field(),
        ]
    ),
    comment="This table contains records of the total transmission capacity nominated for cross-border electricity exchanges between bidding zones or countries, supporting transparency and analysis of cross-border capacity utilization. The data is sourced from ENTSO-E Total Capacity Nominated (12.1.B).",
    sources=[
        TableIdentifier(catalog="bronze", schema="entsoe", name="totalcapacitynominated_12_1_b_r3"),
    ],
    tags={**ENTSOE_COMMON_TAGS, standard_tags.DataSubdomain.KEY: standard_tags.DataSubdomain.TRANSMISSION},
    primary_keys=("delivery_start", "delivery_end", "out_area_code", "in_area_code", "contract_type", "updated_at_source"),
)
