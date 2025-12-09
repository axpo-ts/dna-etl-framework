"""Silver data model definitions."""
from __future__ import annotations

from data_platform.data_model.silver.entsoe.package_a import (
    commercial_exchange_scheduled_table,
    cost_congestion_management_table,
    cross_border_capacity_dc_link_intraday_table,
    physical_flow_cross_border_table,
    redispatching_cross_border_table,
    transfer_capacity_net_dayahead_forecast_table,
    transfer_capacity_nominated_table,
)
from data_platform.data_model.silver.entsoe.package_b import (
    consumption_forecast_table,
    consumption_table,
    hydro_reservoir_level_table,
    production_dayahead_forecast_table,
    production_forecast_table,
    production_output_table,
    production_table,
)
from data_platform.data_model.silver.entsoe.package_c import (
    activated_volume_crossborder_balancing_energy_table,
    bid_balancing_energy_aggregated_table,
    offered_volume_crossborder_balancing_energy_table,
    offered_volume_price_balancing_capacity_table,
    price_balancing_energy_table,
    price_crossborder_balancing_energy_table,
    price_energy_dayahead_table,
    procured_volume_price_balancing_capacity_table,
)
from data_platform.data_model.silver.entsoe.package_d import (
    flow_based_allocation_parameter_table,
    position_net_implicit_allocation_table,
    revenue_auction_explicit_allocation_table,
    transfer_capacity_allocated_third_countries_table,
    transfer_capacity_already_allocated_table,
    transfer_capacity_offered_explicit_table,
    transfer_capacity_offered_implicit_dayahead_table,
    transfer_capacity_use_explicit_allocation_table,
)

from data_platform.data_model.silver.entsoe.entsoe_xml import (
    bid_balancing_energy_archives_table
)

from data_platform.data_model.silver.entsoe.package_e import (
    bid_balancing_energy_table,
)

__all__ = [
    "activated_volume_crossborder_balancing_energy_table",
    "bid_balancing_energy_aggregated_table",
    "bid_balancing_energy_table",
    "commercial_exchange_scheduled_table",
    "consumption_forecast_table",
    "consumption_table",
    "cost_congestion_management_table",
    "cross_border_capacity_dc_link_intraday_table",
    "flow_based_allocation_parameter_table",
    "hydro_reservoir_level_table",
    "offered_volume_crossborder_balancing_energy_table",
    "offered_volume_price_balancing_capacity_table",
    "physical_flow_cross_border_table",
    "position_net_implicit_allocation_table",
    "price_balancing_energy_table",
    "price_crossborder_balancing_energy_table",
    "price_energy_dayahead_table",
    "procured_volume_price_balancing_capacity_legacy_table",
    "procured_volume_price_balancing_capacity_table",
    "production_dayahead_forecast_table",
    "production_forecast_table",
    "production_output_table",
    "production_table",
    "redispatching_cross_border_table",
    "revenue_auction_explicit_allocation_table",
    "transfer_capacity_allocated_third_countries_table",
    "transfer_capacity_already_allocated_table",
    "transfer_capacity_net_dayahead_forecast_table",
    "transfer_capacity_nominated_table",
    "transfer_capacity_offered_explicit_table",
    "transfer_capacity_offered_implicit_dayahead_table",
    "transfer_capacity_use_explicit_allocation_table",
    "bid_balancing_energy_archives_table"
]
