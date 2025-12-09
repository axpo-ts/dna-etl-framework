from __future__ import annotations

from pyspark.sql.types import (
    DoubleType,
    IntegerType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

from data_platform.data_model import StaticTableModel, TableIdentifier
from data_platform.data_model.metadata_common import standard_columns, standard_tags
from data_platform.data_model.silver.entsoe.entsoe_common import ENTSOE_COMMON_TAGS, entsoe_standard_columns


# Procured Volume Price Balancing Capacity Table
procured_volume_price_balancing_capacity_table = StaticTableModel(
    identifier=TableIdentifier(catalog="silver", schema="entsoe", name="procured_volume_price_balancing_capacity"),
    schema=StructType(
        [
            StructField("instance_code", StringType(), False, {"comment": "A unique code identifying a particular instance"}),
            standard_columns.DeliveryStartColumn.to_struct_field(),
            standard_columns.DeliveryEndColumn.to_struct_field(),
            StructField("time_series_start", TimestampType(), False, {"comment": "Start of time series"}),
            StructField("time_series_end", TimestampType(), False, {"comment": "End of time series"}),
            entsoe_standard_columns.AreaCodeColumn.to_struct_field(),
            StructField("area_display_name", StringType(), True, {"comment": "The name of the geographic area, market zone, or control area relevant to the data record"}),
            entsoe_standard_columns.AreaTypeCodeColumn.to_struct_field(),
            StructField("area_map_code", StringType(), True, {"comment": "A standardized code that uniquely identifies a market area, bidding zone, country, or TSO control area within the ENTSO-E data model"}),
            StructField("time_horizon", StringType(), True, {"comment": "Time horizon"}),
            StructField("reserve_type", StringType(), True, {"comment": "The category of ancillary service or reserve product to which the data record pertains, e.g. FCR, aFRR"}),
            StructField("type_of_product", StringType(), True, {"comment": "Type of product"}),
            StructField("reserve_source", StringType(), True, {"comment": "The origin of the reserve capacity in the data record, e.g. Generation"}),
            StructField("direction", StringType(), True, {"comment": "Direction (Up/Down/Symmetric)"}),
            StructField("volume", DoubleType(), True, {"comment": "Volume in MW"}),
            StructField("unit_volume", StringType(), True, {"comment": "The unit of measurement of volume"}),
            StructField("price", DoubleType(), True, {"comment": "Price per MW per ISP"}),
            StructField("currency", StringType(), True, {"comment": "The monetary unit associated with the data record"}),
            StructField("unit_price", StringType(), False, {"comment": "Unit of measurement: currency/MW/ISP"}),
            standard_columns.CommodityColumn.to_struct_field(),
            standard_columns.LicenseColumn.to_struct_field(),
            standard_columns.DataSourceColumn.to_struct_field(),
            standard_columns.DataSystemColumn.to_struct_field(),
            standard_columns.UpdatedAtSourceColumn.to_struct_field(),
        ]
    ),
    comment="This table contains the procured volumes and corresponding prices for balancing capacity contracted by TSOs, organized by product type (e.g., FCR, aFRR, mFRR), procurement period, and market area. Each record captures the capacity volume secured and the awarded price, enabling analysis of capacity adequacy, procurement costs, and market dynamics. The data is sourced from ENTSO-E Procured balancing capacity (12.3.F)",
    sources=[
        TableIdentifier(catalog="bronze", schema="entsoe", name="procuredbalancingcapacity_12_3_f_r3"),
    ],
    tags={**ENTSOE_COMMON_TAGS, standard_tags.DataSubdomain.KEY: standard_tags.DataSubdomain.BALANCING_AND_RESERVE},
    primary_keys=("instance_code", "delivery_start", "delivery_end", "time_series_start", "time_series_end", "area_code", "updated_at_source"),
)

# Price Balancing Energy Table
price_balancing_energy_table = StaticTableModel(
    identifier=TableIdentifier(catalog="silver", schema="entsoe", name="price_balancing_energy"),
    schema=StructType(
        [
            StructField("delivery_start", TimestampType(), False, {"comment": "Imbalance Settlement Period start in UTC"}),
            StructField("delivery_end", TimestampType(), False, {"comment": "End of delivery period"}),
            standard_columns.DurationColumn.to_struct_field(),
            entsoe_standard_columns.AreaCodeColumn.to_struct_field(),
            StructField("area_display_name", StringType(), True, {"comment": "The name of the geographic area, market zone, or control area relevant to the data record"}),
            entsoe_standard_columns.AreaTypeCodeColumn.to_struct_field(),
            entsoe_standard_columns.AreaMapCodeColumn.to_struct_field(),
            StructField("reserve_type", StringType(), True, {"comment": "The category of ancillary service or reserve product to which the data record pertains, e.g. FCR, aFRR"}),
            StructField("type_of_product", StringType(), True, {"comment": "Product type"}),
            StructField("load_up_price", DoubleType(), True, {"comment": "Load up regulation price"}),
            StructField("load_down_price", DoubleType(), True, {"comment": "Load down regulation price"}),
            StructField("generation_up_price", DoubleType(), True, {"comment": "Generation up regulation price"}),
            StructField("generation_down_price", DoubleType(), True, {"comment": "Generation down regulation price"}),
            StructField("not_specified_up_price", DoubleType(), True, {"comment": "Not specified up regulation price"}),
            StructField("not_specified_down_price", DoubleType(), True, {"comment": "Not specified down regulation price"}),
            StructField("price_type", StringType(), True, {"comment": "Price type"}),
            standard_columns.CurrencyColumn.to_struct_field(),
            standard_columns.UnitColumn.to_struct_field(),
            standard_columns.CommodityColumn.to_struct_field(),
            standard_columns.LicenseColumn.to_struct_field(),
            standard_columns.DataSourceColumn.to_struct_field(),
            standard_columns.DataSystemColumn.to_struct_field(),
            standard_columns.UpdatedAtSourceColumn.to_struct_field(),
        ]
    ),
    comment="This table contains records of energy prices for activated balancing energy in electricity markets. Each entry details the up and down prices, product type, reverse type, and delivery period, supporting transparency and analysis of the cost dynamics associated with balancing energy activation. The data is sourced from ENTSO-E Energy Prices of Activated Balancing Energy (17.1.F).",
    sources=[
        TableIdentifier(catalog="bronze", schema="entsoe", name="pricesofactivatedbalancingenergy_17_1_f_r3"),
    ],
    tags={**ENTSOE_COMMON_TAGS, standard_tags.DataSubdomain.KEY: standard_tags.DataSubdomain.BALANCING_AND_RESERVE},
    primary_keys=("delivery_start", "delivery_end", "area_code", "reserve_type", "type_of_product", "updated_at_source"),
)

# Activated Volume Cross-border Balancing Energy Table
activated_volume_crossborder_balancing_energy_table = StaticTableModel(
    identifier=TableIdentifier(catalog="silver", schema="entsoe", name="activated_volume_crossborder_balancing_energy"),
    schema=StructType(
        [
            standard_columns.DeliveryStartColumn.to_struct_field(),
            standard_columns.DeliveryEndColumn.to_struct_field(),
            standard_columns.DurationColumn.to_struct_field(),
            StructField("out_area_code", StringType(), False, {"comment": "The area code where power is flowing out of"}),
            StructField("out_area_type_code", StringType(), True, {"comment": "The type of the area where power is flowing out of"}),
            StructField("out_area_name", StringType(), True, {"comment": "The area name where power is flowing out of"}),
            entsoe_standard_columns.OutAreaMapCodeColumn.to_struct_field(),
            StructField("in_area_code", StringType(), False, {"comment": "The area code where power is flowing into"}),
            StructField("in_area_type_code", StringType(), True, {"comment": "The type of the area where power is flowing in"}),
            StructField("in_area_name", StringType(), True, {"comment": "The area name where power is flowing in"}),
            entsoe_standard_columns.InAreaMapCodeColumn.to_struct_field(),
            StructField("up_activated_energy", DoubleType(), True, {"comment": "The total volume of upward activated energy"}),
            StructField("down_activated_energy", DoubleType(), True, {"comment": "The total volume of downward activated energy"}),
            standard_columns.UnitColumn.to_struct_field(),
            standard_columns.CommodityColumn.to_struct_field(),
            standard_columns.LicenseColumn.to_struct_field(),
            standard_columns.DataSourceColumn.to_struct_field(),
            standard_columns.DataSystemColumn.to_struct_field(),
            standard_columns.UpdatedAtSourceColumn.to_struct_field(),
        ]
    ),
    comment="This table contains records the activated volumes of cross-border balancing energy exchanged between market balance areas (MBAs) across different bidding zones or countries. It is used to monitor and report the actual energy volumes activated for balancing purposes, supporting transparency in cross-border electricity market operations. The data is sourced from ENTSO-E Cross border balancing - Energy Activated (17.1.J)",
    sources=[
        TableIdentifier(catalog="bronze", schema="entsoe", name="crossborderbalancingenergyactivated_17_1_j"),
    ],
    tags={**ENTSOE_COMMON_TAGS, standard_tags.DataSubdomain.KEY: standard_tags.DataSubdomain.BALANCING_AND_RESERVE},
    primary_keys=("delivery_start", "delivery_end", "out_area_code", "in_area_code", "updated_at_source"),
)

# Price Cross-border Balancing Energy Table
price_crossborder_balancing_energy_table = StaticTableModel(
    identifier=TableIdentifier(catalog="silver", schema="entsoe", name="price_crossborder_balancing_energy"),
    schema=StructType(
        [
            standard_columns.DeliveryStartColumn.to_struct_field(),
            standard_columns.DeliveryEndColumn.to_struct_field(),
            standard_columns.DurationColumn.to_struct_field(),
            StructField("out_area_code", StringType(), False, {"comment": "The area code where power is flowing out of"}),
            StructField("out_area_type_code", StringType(), True, {"comment": "The type of the area where power is flowing out of"}),
            StructField("out_area_name", StringType(), True, {"comment": "The area name where power is flowing out of"}),
            entsoe_standard_columns.OutAreaMapCodeColumn.to_struct_field(),
            StructField("in_area_code", StringType(), False, {"comment": "The area code where power is flowing into"}),
            StructField("in_area_type_code", StringType(), True, {"comment": "The type of the area where power is flowing in"}),
            StructField("in_area_name", StringType(), True, {"comment": "The area name where power is flowing in"}),
            entsoe_standard_columns.InAreaMapCodeColumn.to_struct_field(),
            StructField("up_min_price", DoubleType(), True, {"comment": "The minimum price of the upward energy"}),
            StructField("up_max_price", DoubleType(), True, {"comment": "The maximum price of the upward energy"}),
            standard_columns.CurrencyColumn.to_struct_field(),
            standard_columns.CommodityColumn.to_struct_field(),
            standard_columns.LicenseColumn.to_struct_field(),
            standard_columns.DataSourceColumn.to_struct_field(),
            standard_columns.DataSystemColumn.to_struct_field(),
            standard_columns.UpdatedAtSourceColumn.to_struct_field(),
        ]
    ),
    comment="This table contains the prices associated with cross-border balancing energy activated between Market Balance Areas (MBAs) across different bidding zones or countries. Each record captures the price for activated balancing energy over defined time intervals and directions of exchange, enabling analysis of cross-border balancing costs and market dynamics. The table supports transparency and reporting on cross-border balancing operations. The data is sourced from ENTSO-E Cross-Border Balancing (17.1.J)",
    sources=[
        TableIdentifier(catalog="bronze", schema="entsoe", name="crossborderbalancingprices_17_1_j"),
    ],
    tags={**ENTSOE_COMMON_TAGS, standard_tags.DataSubdomain.KEY: standard_tags.DataSubdomain.BALANCING_AND_RESERVE},
    primary_keys=("delivery_start", "delivery_end", "out_area_code", "in_area_code", "updated_at_source"),
)

# Offered Volume Cross-border Balancing Energy Table
offered_volume_crossborder_balancing_energy_table = StaticTableModel(
    identifier=TableIdentifier(catalog="silver", schema="entsoe", name="offered_volume_crossborder_balancing_energy"),
    schema=StructType(
        [
            standard_columns.DeliveryStartColumn.to_struct_field(),
            standard_columns.DeliveryEndColumn.to_struct_field(),
            standard_columns.DurationColumn.to_struct_field(),
            StructField("out_area_code", StringType(), False, {"comment": "The area code where power is flowing out of"}),
            StructField("out_area_type_code", StringType(), True, {"comment": "The type of the area where power is flowing out of"}),
            StructField("out_area_name", StringType(), True, {"comment": "The area name where power is flowing out of"}),
            entsoe_standard_columns.OutAreaMapCodeColumn.to_struct_field(),
            StructField("in_area_code", StringType(), False, {"comment": "The area code where power is flowing into"}),
            StructField("in_area_type_code", StringType(), True, {"comment": "The type of the area where power is flowing in"}),
            StructField("in_area_name", StringType(), True, {"comment": "The area name where power is flowing in"}),
            entsoe_standard_columns.InAreaMapCodeColumn.to_struct_field(),
            StructField("up_offered_volume", DoubleType(), True, {"comment": "The volume of upward balancing energy"}),
            StructField("down_offered_volume", DoubleType(), True, {"comment": "The volume of downward balancing energy"}),
            StructField("deleted_flag", StringType(), False, {"comment": "A numeric flag indicating whether the data is deleted or not"}),
            standard_columns.CommodityColumn.to_struct_field(),
            standard_columns.LicenseColumn.to_struct_field(),
            standard_columns.DataSourceColumn.to_struct_field(),
            standard_columns.DataSystemColumn.to_struct_field(),
            standard_columns.UpdatedAtSourceColumn.to_struct_field(),
        ]
    ),
    comment="This table contains records of the offered volumes of cross-border balancing energy made available by market participants or transmission system operators (TSOs) between Market Balance Areas (MBAs). Each entry details the up and down offered volume, supporting transparency and analysis of the supply side of cross-border balancing mechanisms. The data is sourced from ENTSO-E Cross-Border Balancing - Energy Offered (17.1.J).",
    sources=[
        TableIdentifier(catalog="bronze", schema="entsoe", name="crossborderbalancingvolumesofexchangedbidsandoffers_17_1_j"),
    ],
    tags={**ENTSOE_COMMON_TAGS, standard_tags.DataSubdomain.KEY: standard_tags.DataSubdomain.BALANCING_AND_RESERVE},
    primary_keys=("delivery_start", "delivery_end", "out_area_code", "in_area_code", "updated_at_source"),
)

# Offered Volume Price Balancing Capacity Table
offered_volume_price_balancing_capacity_table = StaticTableModel(
    identifier=TableIdentifier(catalog="silver", schema="entsoe", name="offered_volume_price_balancing_capacity"),
    schema=StructType(
        [
            StructField("instance_code", StringType(), True, {"comment": "A unique code identifying a particular instance"}),
            StructField("contracted_time_period_start", TimestampType(), False, {"comment": "The timestamp of the start of the contracted period in UTC"}),
            StructField("contracted_time_period_end", TimestampType(), False, {"comment": "The timestamp of the end of the contracted period in UTC"}),
            StructField("procurement_date_and_time", TimestampType(), False, {"comment": "The timestamp of the procurement in UTC"}),
            entsoe_standard_columns.AreaCodeColumn.to_struct_field(),
            StructField("area_display_name", StringType(), True, {"comment": "The name of the geographic area, market zone, or control area relevant to the data record"}),
            entsoe_standard_columns.AreaTypeCodeColumn.to_struct_field(),
            entsoe_standard_columns.AreaMapCodeColumn.to_struct_field(),
            StructField("time_horizon", StringType(), True, {"comment": "The granularity of the contracted time period"}),
            StructField("reserve_type", StringType(), True, {"comment": "The category of ancillary service or reserve product to which the data record pertains, e.g. FCR, aFRR"}),
            StructField("type_of_product", StringType(), True, {"comment": "The type of product e.g. Local"}),
            StructField("reserve_source", StringType(), False, {"comment": "The origin of the reserve capacity in the data record, e.g. Generation"}),
            StructField("direction", StringType(), True, {"comment": "The direction of the balancing capacity offered in the market, e.g. Up"}),
            standard_columns.DurationColumn.to_struct_field(),
            StructField("isp_timestamp", StringType(), True, {"comment": "The imbalance settlement period start in UTC"}),
            StructField("volume", DoubleType(), True, {"comment": "The volume per imbalance settlement period"}),
            StructField("unit_volume", StringType(), True, {"comment": "The unit of measurement of volume"}),
            StructField("price", DoubleType(), True, {"comment": "The price per imbalance settlement period"}),
            standard_columns.CurrencyColumn.to_struct_field(),
            StructField("price_type", StringType(), False, {"comment": "The type of the price e.g. AVERAGE"}),
            StructField("unit_price", StringType(), False, {"comment": "Unit of measurement: currency/MW/ISP"}),
            standard_columns.CommodityColumn.to_struct_field(),
            standard_columns.LicenseColumn.to_struct_field(),
            standard_columns.DataSourceColumn.to_struct_field(),
            standard_columns.DataSystemColumn.to_struct_field(),
            standard_columns.UpdatedAtSourceColumn.to_struct_field(),
        ]
    ),
    comment="This table contains records of the offered volumes and prices of contracted balancing capacity reserves submitted by market participants. Each entry details the volume, associated price, product type, and delivery period, supporting transparency and analysis of the balancing capacity market. The data is sourced from ENTSO-E Capacity Volumes and Prices of Contracted Reserves (17.1.B & C).",
    sources=[
        TableIdentifier(catalog="bronze", schema="entsoe", name="amountandpricespaidofbalancingreservesundercontract_17_1_b_c_r3"),
    ],
    tags={**ENTSOE_COMMON_TAGS, standard_tags.DataSubdomain.KEY: standard_tags.DataSubdomain.BALANCING_AND_RESERVE},
    primary_keys=("instance_code", "contracted_time_period_start", "contracted_time_period_end", "isp_timestamp", "area_code", "reserve_type", "reserve_source", "direction", "updated_at_source"),
)

# Bid Balancing Volume Legacy Table
bid_balancing_energy_aggregated_table = StaticTableModel(
    identifier=TableIdentifier(catalog="silver", schema="entsoe", name="bid_balancing_energy_aggregated"),
    schema=StructType(
        [
            StructField("delivery_start", TimestampType(), False, {"comment": "The datetime of the start of the delivery period in UTC"}),
            StructField("delivery_end", TimestampType(), False, {"comment": "The datetime of the end of the delivery period in UTC"}),
            standard_columns.DurationColumn.to_struct_field(),
            entsoe_standard_columns.AreaCodeColumn.to_struct_field(),
            StructField("area_display_name", StringType(), True, {"comment": "The name of the geographic area, market zone, or control area relevant to the data record"}),
            entsoe_standard_columns.AreaTypeCodeColumn.to_struct_field(),
            entsoe_standard_columns.AreaMapCodeColumn.to_struct_field(),
            StructField("reserve_type", StringType(), True, {"comment": "The category of ancillary service or reserve product to which the data record pertains, e.g. FCR, aFRR"}),
            StructField("type_of_product", StringType(), True, {"comment": "The type of product e.g. Local"}),
            StructField("offered_up_bid_volume", DoubleType(), True, {"comment": "The volume of upward offered bid balancing energy"}),
            StructField("offered_down_bid_volume", DoubleType(), True, {"comment": "The volume of downward offered bid balancing energy"}),
            StructField("activated_up_bid_volume", DoubleType(), True, {"comment": "The volume of upward activated bid balancing energy"}),
            StructField("activated_down_bid_volume", DoubleType(), True, {"comment": "The volume of downward activated bid balancing energy"}),
            StructField("unavailable_up_bid_volume", DoubleType(), True, {"comment": "The volume of upward unavailable bid balancing energy"}),
            StructField("unavailable_down_bid_volume", DoubleType(), True, {"comment": "The volume of downward unavailable bid balancing energy"}),
            standard_columns.UnitColumn.to_struct_field(),
            standard_columns.CommodityColumn.to_struct_field(),
            standard_columns.LicenseColumn.to_struct_field(),
            standard_columns.DataSourceColumn.to_struct_field(),
            standard_columns.DataSystemColumn.to_struct_field(),
            standard_columns.UpdatedAtSourceColumn.to_struct_field(),
        ]
    ),
    comment="This table contains aggregated records of balancing energy bids submitted by market participants across different scheduling areas and reserve types. Each entry details the total volumes of offered, activated, and unavailable balancing energy bids, categorized by type of reserves (aFRR, mFRR, RR), direction (up/down), product type (standard/specific), and scheduling area. The data is sourced from ENTSO-E Aggregated Balancing Energy Bids (12.3.E).",
    sources=[
        TableIdentifier(catalog="bronze", schema="entsoe", name="aggregatedbalancingenergybids_12_3_e_r3"),
    ],
    tags={**ENTSOE_COMMON_TAGS, standard_tags.DataSubdomain.KEY: standard_tags.DataSubdomain.BALANCING_AND_RESERVE},
    primary_keys=("delivery_start", "delivery_end", "area_code", "updated_at_source"),
)

# Day Ahead Energy Prices Table
price_energy_dayahead_table = StaticTableModel(
    identifier=TableIdentifier(catalog="silver", schema="entsoe", name="price_energy_dayahead"),
    schema=StructType(
        [
            StructField("instance_code", StringType(), False, {"comment": "A unique code identifying a particular instance"}),
            StructField("delivery_start", TimestampType(), False, {"comment": "The datetime of the start of the delivery period in UTC"}),
            StructField("delivery_end", TimestampType(), False, {"comment": "The datetime of the end of the delivery period in UTC"}),
            standard_columns.DurationColumn.to_struct_field(),
            entsoe_standard_columns.AreaCodeColumn.to_struct_field(),
            StructField("area_display_name", StringType(), True, {"comment": "The name of the geographic area, market zone, or control area relevant to the data record"}),
            entsoe_standard_columns.AreaTypeCodeColumn.to_struct_field(),
            entsoe_standard_columns.AreaMapCodeColumn.to_struct_field(),
            StructField("contract_type", StringType(), True, {"comment": "The type of the contract, e.g. Day-ahead"}),
            StructField("sequence", IntegerType(), True, {"comment": "A numeric value used to define the order of entries within a particular classification"}),
            StructField("price", DoubleType(), True, {"comment": "The day-ahead price in the specified unit"}),
            standard_columns.CurrencyColumn.to_struct_field(),
            standard_columns.UnitColumn.to_struct_field(),
            standard_columns.CommodityColumn.to_struct_field(),
            standard_columns.LicenseColumn.to_struct_field(),
            standard_columns.DataSourceColumn.to_struct_field(),
            standard_columns.DataSystemColumn.to_struct_field(),
            standard_columns.UpdatedAtSourceColumn.to_struct_field(),
        ]
    ),
    comment="This table contains records of day-ahead electricity prices for various bidding zones. Each entry details the price, delivery period, and relevant bidding zone information, supporting transparency and analysis of day-ahead electricity markets. The data is sourced from ENTSO-E Energy Prices (12.1.D).",
    sources=[
        TableIdentifier(catalog="bronze", schema="entsoe", name="energyprices_12_1_d_r3"),
    ],
    tags={**ENTSOE_COMMON_TAGS, standard_tags.DataSubdomain.KEY: standard_tags.DataSubdomain.SPOT},
    primary_keys=("instance_code","delivery_start", "delivery_end", "area_code", "updated_at_source"),
)
