from __future__ import annotations

from pyspark.sql.types import (
    DoubleType,
    IntegerType,
    LongType,
    StringType,
    StructField,
    StructType,
)

from data_platform.data_model import StaticTableModel, TableIdentifier
from data_platform.data_model.metadata_common import standard_columns, standard_tags
from data_platform.data_model.silver.entsoe.entsoe_common import ENTSOE_COMMON_TAGS, entsoe_standard_columns

# Flow Based Allocation Parameter Table
flow_based_allocation_parameter_table = StaticTableModel(
    identifier=TableIdentifier(catalog="silver", schema="entsoe", name="flow_based_allocation_parameter"),
    schema=StructType(
        [
            standard_columns.DeliveryStartColumn.to_struct_field(),
            standard_columns.DeliveryEndColumn.to_struct_field(),
            entsoe_standard_columns.AreaCodeColumn.to_struct_field(),
            entsoe_standard_columns.AreaTypeCodeColumn.to_struct_field(),
            entsoe_standard_columns.AreaNameColumn.to_struct_field(),
            entsoe_standard_columns.AreaMapCodeColumn.to_struct_field(),
            StructField("cbco", LongType(), True, {"comment": "Critical Branch/Critical Outage combination"}),
            StructField("ptdf", DoubleType(), True, {"comment": "Power Transfer Distribution Factor"}),
            StructField("ram", DoubleType(), True, {"comment": "Remaining Available Margin"}),
            standard_columns.UnitColumn.to_struct_field(),
            standard_columns.CommodityColumn.to_struct_field(),
            standard_columns.LicenseColumn.to_struct_field(),
            standard_columns.DataSourceColumn.to_struct_field(),
            standard_columns.DataSystemColumn.to_struct_field(),
            standard_columns.UpdatedAtSourceColumn.to_struct_field(),
        ]
    ),
    comment="This table contains records of parameters used in flow-based capacity allocation for cross-border electricity markets. The table enables transparency and analysis of the flow-based allocation process. The data is sourced from ENTSO-E Flow-based Allocations (11.1).",
    sources=[TableIdentifier(catalog="bronze", schema="entsoe", name="dayaheadflowbasedallocations_11_1")],
    tags={**ENTSOE_COMMON_TAGS, standard_tags.DataSubdomain.KEY: standard_tags.DataSubdomain.TRANSMISSION},
    primary_keys=("delivery_start", "delivery_end", "area_code", "cbco", "updated_at_source"),
)

# Transfer Capacity Offered Implicit Day-ahead Table
transfer_capacity_offered_implicit_dayahead_table = StaticTableModel(
    identifier=TableIdentifier(catalog="silver", schema="entsoe", name="transfer_capacity_offered_implicit_dayahead"),
    schema=StructType(
        [
            StructField("instance_code", StringType(), True, {"comment": "A unique code identifying a particular instance"}),
            standard_columns.DeliveryStartColumn.to_struct_field(),
            standard_columns.DeliveryEndColumn.to_struct_field(),
            standard_columns.DurationColumn.to_struct_field(),
            StructField("contract_type", StringType(), True, {"comment": "The type of the contract, e.g. Day-ahead"}),
            StructField("sequence", StringType(), True, {"comment": "A numeric value used to define the order of entries within a particular classification"}),
            StructField("out_area_code", StringType(), False, {"comment": "The area code where power is flowing out of"}),
            StructField("out_area_name", StringType(), True, {"comment": "The area name where power is flowing out of"}),
            StructField("out_area_type_code", StringType(), True, {"comment": "The type of the area where power is flowing out of"}),
            StructField("out_area_map_code", StringType(), False, {"comment": "The area code where power is flowing out of"}),
            StructField("in_area_code", StringType(), False, {"comment": "The area code where power is flowing into"}),
            StructField("in_area_name", StringType(), True, {"comment": "The area name where power is flowing in"}),
            StructField("in_area_type_code", StringType(), True, {"comment": "The type of the area where power is flowing in"}),
            StructField("in_area_map_code", StringType(), False, {"comment": "The area code where power is flowing into"}),
            StructField("capacity", DoubleType(), True, {"comment": "The day-ahead offered capacity between 2 areas, in the specified unit"}),
            standard_columns.UnitColumn.to_struct_field(),
            standard_columns.CommodityColumn.to_struct_field(),
            standard_columns.LicenseColumn.to_struct_field(),
            standard_columns.DataSourceColumn.to_struct_field(),
            standard_columns.DataSystemColumn.to_struct_field(),
            standard_columns.UpdatedAtSourceColumn.to_struct_field(),
        ]
    ),
    comment="This table reports the transfer capacities offered through implicit allocation in the day-ahead market on each interconnector border and direction. Each record captures the offered capacity for a specific delivery interval, supporting transparency on available cross-border capacity and day-ahead market coupling. The data is sourced from ENTSO-E Offered Transfer Capacities - Implicit (Day-Ahead) (11.1)",
    sources=[TableIdentifier(catalog="bronze", schema="entsoe", name="offeredtransfercapacitiesimplicit_11_1_r3")],
    tags={**ENTSOE_COMMON_TAGS, standard_tags.DataSubdomain.KEY: standard_tags.DataSubdomain.TRANSMISSION},
    primary_keys=("instance_code", "delivery_start", "delivery_end", "out_area_code", "out_area_type_code", "in_area_code", "in_area_type_code", "updated_at_source"),
)

# Transfer Capacity Offered Explicit Table
transfer_capacity_offered_explicit_table = StaticTableModel(
    identifier=TableIdentifier(catalog="silver", schema="entsoe", name="transfer_capacity_offered_explicit"),
    schema=StructType(
        [
            StructField("instance_code", StringType(), True, {"comment": "A unique code identifying a particular instance"}),
            standard_columns.DeliveryStartColumn.to_struct_field(),
            standard_columns.DeliveryEndColumn.to_struct_field(),
            standard_columns.DurationColumn.to_struct_field(),
            StructField("contract_type", StringType(), True, {"comment": "The type of the contract, e.g. Day-ahead"}),
            StructField("category", StringType(), True, {"comment": "The classification assigned to the data record, e.g. Hourly"}),
            StructField("sequence", IntegerType(), True, {"comment": "A numeric value used to define the order of entries within a particular classification"}),
            StructField("out_area_code", StringType(), False, {"comment": "The area code where power is flowing out of"}),
            StructField("out_area_display_name", StringType(), True, {"comment": "The area name where power is flowing out of"}),
            StructField("out_area_type_code", StringType(), True, {"comment": "The type of the area where power is flowing out of"}),
            entsoe_standard_columns.OutAreaMapCodeColumn.to_struct_field(),
            StructField("in_area_code", StringType(), False, {"comment": "The area code where power is flowing into"}),
            StructField("in_area_display_name", StringType(), True, {"comment": "The area name where power is flowing in"}),
            StructField("in_area_type_code", StringType(), True, {"comment": "The type of the area where power is flowing in"}),
            entsoe_standard_columns.InAreaMapCodeColumn.to_struct_field(),
            StructField("capacity", IntegerType(), True, {"comment": "The offered capacity between 2 areas, in the specified unit"}),
            standard_columns.UnitColumn.to_struct_field(),
            standard_columns.CommodityColumn.to_struct_field(),
            standard_columns.LicenseColumn.to_struct_field(),
            standard_columns.DataSourceColumn.to_struct_field(),
            standard_columns.DataSystemColumn.to_struct_field(),
            standard_columns.UpdatedAtSourceColumn.to_struct_field(),
        ]
    ),
    comment="This table reports the transfer capacities offered through explicit allocation mechanisms on interconnectors, typically by border and direction across relevant market timeframes (e.g., intraday). Each record captures the offered capacity for a given auction or time interval, enabling transparency on capacity availability and cross-border trading opportunities. The data is sourced from ENTSO-E Offered Transfer Capacities - Explicit (11.1)",
    sources=[TableIdentifier(catalog="bronze", schema="entsoe", name="offeredtransfercapacitiesexplicit_11_1_r3")],
    tags={**ENTSOE_COMMON_TAGS, standard_tags.DataSubdomain.KEY: standard_tags.DataSubdomain.TRANSMISSION},
    primary_keys=("delivery_start", "delivery_end", "out_area_code", "in_area_code", "contract_type", "sequence", "updated_at_source"),
)

# Revenue Auction Explicit Allocation Table
revenue_auction_explicit_allocation_table = StaticTableModel(
    identifier=TableIdentifier(catalog="silver", schema="entsoe", name="revenue_auction_explicit_allocation"),
    schema=StructType(
        [
            standard_columns.DeliveryStartColumn.to_struct_field(),
            standard_columns.DeliveryEndColumn.to_struct_field(),
            standard_columns.DurationColumn.to_struct_field(),
            StructField("allocation_id", StringType(), True, {"comment": "A unique identifier for a specific capacity allocation event"}),
            StructField("allocation_mode_code", StringType(), True, {"comment": "A standardized code describing the method by which cross-border transmission capacity are allocated"}),
            StructField("capacity_product_code", StringType(), True, {"comment": "The type or category of transmission capacity product being allocated or traded"}),
            StructField("aggregated", IntegerType(), True, {"comment": "An integer indicator showing whether the data is aggregated or not"}),
            StructField("out_area_code", StringType(), False, {"comment": "The area code where power is flowing out of"}),
            StructField("out_area_type_code", StringType(), True, {"comment": "The type of the area where power is flowing out of"}),
            StructField("out_area_name", StringType(), True, {"comment": "The area name where power is flowing out of"}),
            entsoe_standard_columns.OutAreaMapCodeColumn.to_struct_field(),
            StructField("in_area_code", StringType(), False, {"comment": "The area code where power is flowing into"}),
            StructField("in_area_type_code", StringType(), True, {"comment": "The type of the area where power is flowing in"}),
            StructField("in_area_name", StringType(), True, {"comment": "The area name where power is flowing in"}),
            entsoe_standard_columns.InAreaMapCodeColumn.to_struct_field(),
            StructField("revenue", DoubleType(), True, {"comment": "The auction revenue (in Currency) per border between bidding zones"}),
            standard_columns.CurrencyColumn.to_struct_field(),
            standard_columns.CommodityColumn.to_struct_field(),
            standard_columns.LicenseColumn.to_struct_field(),
            standard_columns.DataSourceColumn.to_struct_field(),
            standard_columns.DataSystemColumn.to_struct_field(),
            standard_columns.UpdatedAtSourceColumn.to_struct_field(),
        ]
    ),
    comment="This table records auction revenues from explicit cross-border capacity allocations in the intraday timeframe. Each entry captures the revenue realized for a specific border,and time interval, enabling transparency and analysis of congestion rents and market performance for explicit allocations. The data is sourced from ENTSO-E Explicit Allocations Auction Revenue Daily (12.3.A)",
    sources=[TableIdentifier(catalog="bronze", schema="entsoe", name="explicitallocationsauctionrevenuedaily_12_3_a")],
    tags={**ENTSOE_COMMON_TAGS, standard_tags.DataSubdomain.KEY: standard_tags.DataSubdomain.TRANSMISSION},
    primary_keys=("delivery_start", "delivery_end", "out_area_code", "in_area_code", "allocation_id", "updated_at_source"),
)

# Transfer Capacity Use Explicit Allocation Table
transfer_capacity_use_explicit_allocation_table = StaticTableModel(
    identifier=TableIdentifier(catalog="silver", schema="entsoe", name="transfer_capacity_use_explicit_allocation"),
    schema=StructType(
        [
            standard_columns.DeliveryStartColumn.to_struct_field(),
            standard_columns.DeliveryEndColumn.to_struct_field(),
            standard_columns.DurationColumn.to_struct_field(),
            StructField("allocation_id", StringType(), True, {"comment": "A unique identifier for a specific capacity allocation event"}),
            StructField("allocation_mode_code", StringType(), True, {"comment": "A standardized code describing the method by which cross-border transmission capacity are allocated"}),
            StructField("capacity_product_code", StringType(), True, {"comment": "The type or category of transmission capacity product being allocated or traded"}),
            StructField("out_area_code", StringType(), False, {"comment": "The area code where power is flowing out of"}),
            StructField("out_area_type_code", StringType(), True, {"comment": "The type of the area where power is flowing out of"}),
            StructField("out_area_name", StringType(), True, {"comment": "The area name where power is flowing out of"}),
            entsoe_standard_columns.OutAreaMapCodeColumn.to_struct_field(),
            StructField("in_area_code", StringType(), False, {"comment": "The area code where power is flowing into"}),
            StructField("in_area_type_code", StringType(), True, {"comment": "The type of the area where power is flowing in"}),
            StructField("in_area_name", StringType(), True, {"comment": "The area name where power is flowing in"}),
            entsoe_standard_columns.InAreaMapCodeColumn.to_struct_field(),
            StructField("allocated_capacity", DoubleType(), True, {"comment": "The capacity allocated to the market, in the specified unit"}),
            StructField("unit_allocated_capacity", StringType(), False, {"comment": "The unit of measurement of allocated capacity"}),
            StructField("requested_capacity", DoubleType(), True, {"comment": "The capacity requested by the market, in the specified unit"}),
            StructField("unit_requested_capacity", StringType(), False, {"comment": "The unit of measurement of requested capacity"}),
            StructField("capacity_price", DoubleType(), True, {"comment": "The price of the capacity, in currency/MWh"}),
            standard_columns.CurrencyColumn.to_struct_field(),
            StructField("unit_capacity_price", StringType(), False, {"comment": "The unit of measurement of capacity price"}),
            standard_columns.CommodityColumn.to_struct_field(),
            standard_columns.LicenseColumn.to_struct_field(),
            standard_columns.DataSourceColumn.to_struct_field(),
            standard_columns.DataSystemColumn.to_struct_field(),
            standard_columns.UpdatedAtSourceColumn.to_struct_field(),
        ]
    ),
    comment="This table records the actual use of transfer capacity allocated via explicit mechanisms, typically by border and direction in the day-ahead timeframe. Each record captures the utilized capacity for a given delivery interval (e.g., scheduled nominations against allocated rights), enabling transparency on how much of the explicitly allocated capacity was actually used and supporting analysis of congestion and interconnector efficiency. The data is sourced from ENTSO-E Use of the Transfer Capacity (12.1.A)",
    sources=[TableIdentifier(catalog="bronze", schema="entsoe", name="explicitallocationsuseoftransfercapacitydaily_12_1_a")],
    tags={**ENTSOE_COMMON_TAGS, standard_tags.DataSubdomain.KEY: standard_tags.DataSubdomain.TRANSMISSION},
    primary_keys=("delivery_start", "delivery_end", "out_area_code", "in_area_code", "allocation_id", "updated_at_source"),
)

# Transfer Capacity Already Allocated Table
transfer_capacity_already_allocated_table = StaticTableModel(
    identifier=TableIdentifier(catalog="silver", schema="entsoe", name="transfer_capacity_already_allocated"),
    schema=StructType(
        [
            StructField("instance_code", StringType(), True, {"comment": "A unique code identifying a particular instance"}),
            standard_columns.DeliveryStartColumn.to_struct_field(),
            standard_columns.DeliveryEndColumn.to_struct_field(),
            standard_columns.DurationColumn.to_struct_field(),
            StructField("contract_type", StringType(), True, {"comment": "The type of the contract, e.g. Day-ahead"}),
            StructField("category", StringType(), True, {"comment": "The classification assigned to the data record, e.g. Hourly"}),
            StructField("out_area_code", StringType(), False, {"comment": "The area code where power is flowing out of"}),
            StructField("out_area_display_name", StringType(), True, {"comment": "The area name where power is flowing out of"}),
            StructField("out_area_type_code", StringType(), True, {"comment": "The type of the area where power is flowing out of"}),
            StructField("out_area_map_code", StringType(), True, {"comment": "The map code where power is flowing out of"}),
            StructField("in_area_code", StringType(), False, {"comment": "The area code where power is flowing into"}),
            StructField("in_area_display_name", StringType(), True, {"comment": "The area name where power is flowing in"}),
            StructField("in_area_type_code", StringType(), True, {"comment": "The type of the area where power is flowing in"}),
            StructField("in_area_map_code", StringType(), True, {"comment": "The map code where power is flowing in"}),
            StructField("capacity", IntegerType(), True, {"comment": "The total capacity already allocated, in the specified unit"}),
            standard_columns.UnitColumn.to_struct_field(),
            standard_columns.CommodityColumn.to_struct_field(),
            standard_columns.LicenseColumn.to_struct_field(),
            standard_columns.DataSourceColumn.to_struct_field(),
            standard_columns.DataSystemColumn.to_struct_field(),
            standard_columns.UpdatedAtSourceColumn.to_struct_field(),
        ]
    ),
    comment="This table reports the total interconnection capacity already allocated on each border and direction across market timeframes (e.g., day-ahead, intraday). Each record captures the cumulative allocated capacity for a given time interval, supporting transparency on available transfer capacity, congestion management, and cross-border market usage. The data is sourced from ENTSO-E Total Capacity Already Allocated (12.1.C)",
    sources=[TableIdentifier(catalog="bronze", schema="entsoe", name="totalcapacityalreadyallocated_12_1_c_r3")],
    tags={**ENTSOE_COMMON_TAGS, standard_tags.DataSubdomain.KEY: standard_tags.DataSubdomain.TRANSMISSION},
    primary_keys=("delivery_start", "delivery_end", "out_area_code", "in_area_code", "contract_type", "updated_at_source"),
)

# Position Net Implicit Allocation Table
position_net_implicit_allocation_table = StaticTableModel(
    identifier=TableIdentifier(catalog="silver", schema="entsoe", name="position_net_implicit_allocation"),
    schema=StructType(
        [
            standard_columns.DeliveryStartColumn.to_struct_field(),
            standard_columns.DeliveryEndColumn.to_struct_field(),
            standard_columns.DurationColumn.to_struct_field(),
            entsoe_standard_columns.AreaCodeColumn.to_struct_field(),
            StructField("area_display_name", StringType(), True, {"comment": "The name of the geographic area, market zone, or control area relevant to the data record"}),
            entsoe_standard_columns.AreaTypeCodeColumn.to_struct_field(),
            entsoe_standard_columns.AreaMapCodeColumn.to_struct_field(),
            StructField("contract_type", StringType(), True, {"comment": "The type of the contract, e.g. Day-ahead"}),
            StructField("net_position", DoubleType(), True, {"comment": "The total implicit net position in the specified unit"}),
            StructField("direction", StringType(), True, {"comment": "Specifies the flow direction, either export or import"}),
            standard_columns.UnitColumn.to_struct_field(),
            standard_columns.CommodityColumn.to_struct_field(),
            standard_columns.LicenseColumn.to_struct_field(),
            standard_columns.DataSourceColumn.to_struct_field(),
            standard_columns.DataSystemColumn.to_struct_field(),
            standard_columns.UpdatedAtSourceColumn.to_struct_field(),
        ]
    ),
    comment="This table contains records of net positions resulting from implicit allocation processes in cross-border electricity markets, supporting transparency and analysis of market outcomes and cross-border trade flows. The data is sourced from ENTSO-E Implicit Allocations - Net Positions (12.1.E).",
    sources=[TableIdentifier(catalog="bronze", schema="entsoe", name="implicitallocationsnetpositions_12_1_e_r3")],
    tags={**ENTSOE_COMMON_TAGS, standard_tags.DataSubdomain.KEY: standard_tags.DataSubdomain.TRANSMISSION},
    primary_keys=("delivery_start", "delivery_end", "area_code", "contract_type", "updated_at_source"),
)

# Transfer Capacity Allocated Third Countries Table
transfer_capacity_allocated_third_countries_table = StaticTableModel(
    identifier=TableIdentifier(catalog="silver", schema="entsoe", name="transfer_capacity_allocated_third_countries"),
    schema=StructType(
        [
            StructField("instance_code", StringType(), True, {"comment": "A unique code identifying a particular instance"}),
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
            StructField("contract_type", StringType(), True, {"comment": "The type of the contract, e.g. Day-ahead"}),
            StructField("category", StringType(), True, {"comment": "The classification assigned to the data record, e.g. Hourly"}),
            StructField("sequence", IntegerType(), True, {"comment": "A numeric value used to define the order of entries within a particular classification"}),
            StructField("allocated_capacity", IntegerType(), True, {"comment": "Transfer capacities allocated between bidding zones, in the specified unit"}),
            standard_columns.UnitColumn.to_struct_field(),
            standard_columns.CommodityColumn.to_struct_field(),
            standard_columns.LicenseColumn.to_struct_field(),
            standard_columns.DataSourceColumn.to_struct_field(),
            standard_columns.DataSystemColumn.to_struct_field(),
            standard_columns.UpdatedAtSourceColumn.to_struct_field(),
        ]
    ),
    comment="This table reports the cross-border transfer capacities allocated on borders with third countries (non-EU/EEA) across market timeframes (e.g., day-ahead, intraday). Each record captures the allocated capacity by border and direction for a given time interval, supporting transparency on interconnection usage and access involving third-country borders. The data is sourced from ENTSO-E Transfer Capacities Allocated with Third Countries (12.1.H)",
    sources=[TableIdentifier(catalog="bronze", schema="entsoe", name="transfercapacitiesallocatedwiththirdcountries_12_1_h_r3")],
    tags={**ENTSOE_COMMON_TAGS, standard_tags.DataSubdomain.KEY: standard_tags.DataSubdomain.TRANSMISSION},
    primary_keys=("delivery_start", "delivery_end", "out_area_code", "in_area_code", "contract_type", "sequence", "updated_at_source"),
)
