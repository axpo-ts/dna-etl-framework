from __future__ import annotations

from pyspark.sql.types import (
    DoubleType,
    IntegerType,
    StringType,
    StructField,
    StructType,
)

from data_platform.data_model import StaticTableModel, TableIdentifier
from data_platform.data_model.metadata_common import standard_columns, standard_tags
from data_platform.data_model.silver.entsoe.entsoe_common import ENTSOE_COMMON_TAGS, entsoe_standard_columns

# Consumption Table
consumption_table = StaticTableModel(
    identifier=TableIdentifier(catalog="silver", schema="entsoe", name="consumption"),
    schema=StructType(
        [
            standard_columns.DeliveryStartColumn.to_struct_field(),
            standard_columns.DeliveryEndColumn.to_struct_field(),
            standard_columns.DurationColumn.to_struct_field(),
            entsoe_standard_columns.AreaCodeColumn.to_struct_field(nullable=False),
            entsoe_standard_columns.AreaTypeCodeColumn.to_struct_field(),
            entsoe_standard_columns.AreaNameColumn.to_struct_field(),
            entsoe_standard_columns.AreaMapCodeColumn.to_struct_field(),
            StructField("total_load_value", DoubleType(), True, {"comment": "The total load per area per market time unit"}),
            standard_columns.UnitColumn.to_struct_field(),
            standard_columns.CommodityColumn.to_struct_field(),
            standard_columns.LicenseColumn.to_struct_field(),
            standard_columns.DataSourceColumn.to_struct_field(),
            standard_columns.DataSystemColumn.to_struct_field(),
            standard_columns.UpdatedAtSourceColumn.to_struct_field(),
        ]
    ),
    comment="This table contains records of actual total electricity load for each market area, providing transparency and supporting analysis of electricity demand patterns. The data is sourced from ENTSO-E Actual total load consumption (6.1.A).",
    sources=[
        TableIdentifier(catalog="bronze", schema="entsoe", name="actualtotalload_6_1_a_r3"),
    ],
    tags={**ENTSOE_COMMON_TAGS, standard_tags.DataSubdomain.KEY: standard_tags.DataSubdomain.CONSUMPTION},
    primary_keys=("delivery_start", "delivery_end", "area_code", "area_type_code", "updated_at_source"),
)

# Consumption Forecast Table
consumption_forecast_table = StaticTableModel(
    identifier=TableIdentifier(catalog="silver", schema="entsoe", name="consumption_forecast"),
    schema=StructType(
        [
            standard_columns.DeliveryStartColumn.to_struct_field(),
            standard_columns.DeliveryEndColumn.to_struct_field(),
            standard_columns.DurationColumn.to_struct_field(),
            entsoe_standard_columns.AreaCodeColumn.to_struct_field(nullable=False),
            entsoe_standard_columns.AreaTypeCodeColumn.to_struct_field(),
            entsoe_standard_columns.AreaNameColumn.to_struct_field(),
            entsoe_standard_columns.AreaMapCodeColumn.to_struct_field(),
            StructField("total_load_value", DoubleType(), True, {"comment": "The total load per area per market time unit"}),
            standard_columns.UnitColumn.to_struct_field(),
            standard_columns.CommodityColumn.to_struct_field(),
            standard_columns.LicenseColumn.to_struct_field(),
            standard_columns.DataSourceColumn.to_struct_field(),
            standard_columns.DataSystemColumn.to_struct_field(),
            standard_columns.UpdatedAtSourceColumn.to_struct_field(),
        ]
    ),
    comment="This table contains records of day-ahead forecasted electricity load for each market area, enabling analysis and transparency of expected electricity demand. The data is sourced from ENTSO-E Day-ahead total load forecast (6.1.B).",
    sources=[
        TableIdentifier(catalog="bronze", schema="entsoe", name="dayaheadtotalloadforecast_6_1_b_r3"),
    ],
    tags={**ENTSOE_COMMON_TAGS, standard_tags.DataSubdomain.KEY: standard_tags.DataSubdomain.CONSUMPTION},
    primary_keys=("delivery_start", "delivery_end", "area_code", "area_type_code", "updated_at_source"),
)

# Production Table
production_table = StaticTableModel(
    identifier=TableIdentifier(catalog="silver", schema="entsoe", name="production"),
    schema=StructType(
        [
            standard_columns.DeliveryStartColumn.to_struct_field(),
            standard_columns.DeliveryEndColumn.to_struct_field(),
            standard_columns.DurationColumn.to_struct_field(),
            entsoe_standard_columns.AreaCodeColumn.to_struct_field(nullable=False),
            entsoe_standard_columns.AreaTypeCodeColumn.to_struct_field(),
            entsoe_standard_columns.AreaNameColumn.to_struct_field(),
            entsoe_standard_columns.AreaMapCodeColumn.to_struct_field(),
            StructField("production_type", StringType(), False, {"comment": "The technology used for electricity generation, e.g. Solar"}),
            StructField("actual_generation_output", DoubleType(), True, {"comment": "Actual aggregated net generation output per market time unit and per production type, in the specified unit"}),
            StructField("actual_consumption", DoubleType(), True, {"comment": "Actual consumption per market time unit and per production type"}),
            standard_columns.UnitColumn.to_struct_field(),
            standard_columns.CommodityColumn.to_struct_field(),
            standard_columns.LicenseColumn.to_struct_field(),
            standard_columns.DataSourceColumn.to_struct_field(),
            standard_columns.DataSystemColumn.to_struct_field(),
            standard_columns.UpdatedAtSourceColumn.to_struct_field(),
        ]
    ),
    comment="This table reports actual electricity generation by production type (e.g., nuclear, hydro, wind, solar, thermal) over time for each bidding zone or control area. Each record includes the measured output for a specific production type and time interval, supporting analyses of generation mix, renewable penetration, and operational reporting. The data is sourced from ENTSO-E Actual Generation per Production Type (16.1.B/C)",
    sources=[
        TableIdentifier(catalog="bronze", schema="entsoe", name="aggregatedgenerationpertype_16_1_b_c_r3"),
    ],
    tags={**ENTSOE_COMMON_TAGS, standard_tags.DataSubdomain.KEY: standard_tags.DataSubdomain.PRODUCTION},
    primary_keys=("delivery_start", "delivery_end", "area_code", "area_type_code", "production_type", "updated_at_source"),
)

# Production Day-ahead Forecast Table
production_dayahead_forecast_table = StaticTableModel(
    identifier=TableIdentifier(catalog="silver", schema="entsoe", name="production_dayahead_forecast"),
    schema=StructType(
        [
            standard_columns.DeliveryStartColumn.to_struct_field(),
            standard_columns.DeliveryEndColumn.to_struct_field(),
            standard_columns.DurationColumn.to_struct_field(),
            entsoe_standard_columns.AreaCodeColumn.to_struct_field(nullable=False),
            entsoe_standard_columns.AreaTypeCodeColumn.to_struct_field(),
            entsoe_standard_columns.AreaNameColumn.to_struct_field(),
            entsoe_standard_columns.AreaMapCodeColumn.to_struct_field(),
            StructField("scheduled_generation", DoubleType(), True, {"comment": "Scheduled generation"}),
            StructField("scheduled_consumption", DoubleType(), True, {"comment": "Scheduled consumption"}),
            standard_columns.UnitColumn.to_struct_field(),
            standard_columns.CommodityColumn.to_struct_field(),
            standard_columns.LicenseColumn.to_struct_field(),
            standard_columns.DataSourceColumn.to_struct_field(),
            standard_columns.DataSystemColumn.to_struct_field(),
            standard_columns.UpdatedAtSourceColumn.to_struct_field(),
        ]
    ),
    comment="This table contains day-ahead forecasts of electricity generation and consumption for each bidding zone or control area, enabling planning, market analysis, and comparison against actual generation. The data is sourced from ENTSO-E Generation Forecast - Day-ahead (14.1.C)",
    sources=[
        TableIdentifier(catalog="bronze", schema="entsoe", name="dayaheadaggregatedgeneration_14_1_c_r3"),
    ],
    tags={**ENTSOE_COMMON_TAGS, standard_tags.DataSubdomain.KEY: standard_tags.DataSubdomain.PRODUCTION},
    primary_keys=("delivery_start", "delivery_end", "area_code", "area_type_code", "updated_at_source"),
)


# Production Forecast (Wind and Solar) Table
production_forecast_table = StaticTableModel(
    identifier=TableIdentifier(catalog="silver", schema="entsoe", name="production_forecast"),
    schema=StructType(
        [
            standard_columns.DeliveryStartColumn.to_struct_field(),
            standard_columns.DeliveryEndColumn.to_struct_field(),
            standard_columns.DurationColumn.to_struct_field(),
            entsoe_standard_columns.AreaCodeColumn.to_struct_field(nullable=False),
            entsoe_standard_columns.AreaNameColumn.to_struct_field(),
            entsoe_standard_columns.AreaTypeCodeColumn.to_struct_field(),
            entsoe_standard_columns.AreaMapCodeColumn.to_struct_field(),
            StructField("production_type", StringType(), False, {"comment": "The technology used for electricity generation, e.g. Solar"}),
            StructField("day_ahead_generation_forecast", DoubleType(), True, {"comment": "Forecasts of the aggregated generation (MW) per bidding zone, per each market time unit"}),
            StructField("intraday_generation_forecast", DoubleType(), True, {"comment": "Forecasts of the aggregated generation (MW) per bidding zone, per each market time unit"}),
            StructField("current_generation_forecast", DoubleType(), True, {"comment": "Forecasts of the aggregated generation (MW) per bidding zone, per each market time unit"}),
            standard_columns.UnitColumn.to_struct_field(),
            standard_columns.CommodityColumn.to_struct_field(),
            standard_columns.LicenseColumn.to_struct_field(),
            standard_columns.DataSourceColumn.to_struct_field(),
            standard_columns.DataSystemColumn.to_struct_field(),
            standard_columns.UpdatedAtSourceColumn.to_struct_field(),
        ]
    ),
    comment="This table contains generation forecasts for variable renewable production (wind and solar) by bidding zone or control area. Each record provides the forecasted aggregated generation for a specific technology (onshore wind, offshore wind, solar) and time interval. The data is sourced from ENTSO-E Generation Forecasts for Wind and Solar (14.1.D)",
    sources=[
        TableIdentifier(catalog="bronze", schema="entsoe", name="generationforecastsforwindandsolar_14_1_d_r3"),
    ],
    tags={**ENTSOE_COMMON_TAGS, standard_tags.DataSubdomain.KEY: standard_tags.DataSubdomain.PRODUCTION},
    primary_keys=("delivery_start", "delivery_end", "area_code", "area_type_code", "production_type", "updated_at_source"),
)

# Hydro Reservoir Level Table
hydro_reservoir_level_table = StaticTableModel(
    identifier=TableIdentifier(catalog="silver", schema="entsoe", name="hydro_reservoir_level"),
    schema=StructType(
        [
            StructField("year", StringType(), True, {"comment": "Year of the record"}),
            StructField("month", StringType(), True, {"comment": "Month of the record"}),
            StructField("week", StringType(), True, {"comment": "Week of the record"}),
            StructField("time_zone", StringType(), True, {"comment": "Time zone"}),
            standard_columns.DurationColumn.to_struct_field(),
            entsoe_standard_columns.AreaCodeColumn.to_struct_field(nullable=False),
            StructField("area_display_name", StringType(), True, {"comment": "Display name of the area"}),
            entsoe_standard_columns.AreaTypeCodeColumn.to_struct_field(),
            entsoe_standard_columns.AreaMapCodeColumn.to_struct_field(),
            StructField("stored_energy", DoubleType(), True, {"comment": "The energy stored in the specific unit"}),
            standard_columns.UnitColumn.to_struct_field(),
            standard_columns.CommodityColumn.to_struct_field(),
            standard_columns.LicenseColumn.to_struct_field(),
            standard_columns.DataSourceColumn.to_struct_field(),
            standard_columns.DataSystemColumn.to_struct_field(),
            standard_columns.UpdatedAtSourceColumn.to_struct_field(),
        ]
    ),
    comment="This table contains records of the aggregate filling rates of water reservoirs and hydro storage plants within each market area, supporting transparency and analysis of hydro storage resources and their availability for electricity generation. The data is sourced from ENTSO-E Aggregate Filling Rate of Water Reservoirs and Hydro Storage Plants (16.1.D).",
    sources=[
        TableIdentifier(catalog="bronze", schema="entsoe", name="aggregatedfillingrateofwaterreservoirsandhydrostorageplants_16_1_d_r3"),
    ],
    tags={**ENTSOE_COMMON_TAGS, standard_tags.DataSubdomain.KEY: standard_tags.DataSubdomain.HYDRO},
    primary_keys=("year", "month", "week", "area_code", "area_type_code", "updated_at_source"),
)

# Production Generation Output Table
production_output_table = StaticTableModel(
    identifier=TableIdentifier(catalog="silver", schema="entsoe", name="production_output"),
    schema=StructType(
        [
            standard_columns.DeliveryStartColumn.to_struct_field(),
            standard_columns.DeliveryEndColumn.to_struct_field(),
            standard_columns.DurationColumn.to_struct_field(),
            entsoe_standard_columns.AreaCodeColumn.to_struct_field(nullable=False),
            StructField("area_display_name", StringType(), True, {"comment": "The name of the geographic area, market zone, or control area relevant to the data record"}),
            entsoe_standard_columns.AreaTypeCodeColumn.to_struct_field(),
            StructField("area_map_code", StringType(), True, {"comment": "A standardized code that uniquely identifies a market area, bidding zone, country, or TSO control area within the ENTSO-E data model"}),
            StructField("generation_unit_code", StringType(), True, {"comment": "A unique alphanumeric identifier for a specific generation unit"}),
            StructField("generation_unit_name", StringType(), True, {"comment": "The name of the generation unit"}),
            StructField("generation_unit_type", StringType(), True, {"comment": "The technology used for electricity generation, e.g. Solar"}),
            StructField("actual_generation_output", DoubleType(), True, {"comment": "Actual aggregated net generation output per market time unit and per production type, in the specified unit"}),
            StructField("actual_consumption", DoubleType(), True, {"comment": "Actual consumption per market time unit and per production type"}),
            standard_columns.UnitColumn.to_struct_field(),
            standard_columns.CommodityColumn.to_struct_field(),
            standard_columns.LicenseColumn.to_struct_field(),
            standard_columns.DataSourceColumn.to_struct_field(),
            standard_columns.DataSystemColumn.to_struct_field(),
            standard_columns.UpdatedAtSourceColumn.to_struct_field(),
        ]
    ),
    comment="This table contains records of actual net electricity generation output for individual generation units across European power systems. Each entry details the real-time generation output in MW per market time unit for generation units with installed capacity of 100 MW or more, including unit identifiers, production type, location, and temporal data. The data is sourced from ENTSO-E Actual Generation Output per Generation Unit (16.1.A).",
    sources=[
        TableIdentifier(catalog="bronze", schema="entsoe", name="actualgenerationoutputpergenerationunit_16_1_a_r3"),
    ],
    tags={**ENTSOE_COMMON_TAGS, standard_tags.DataSubdomain.KEY: standard_tags.DataSubdomain.PRODUCTION},
    primary_keys=("delivery_start", "delivery_end", "area_code", "area_type_code", "updated_at_source"),
)
