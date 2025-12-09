"""Silver data model definitions."""
from __future__ import annotations

from data_platform.data_model.silver.volue.attribute import (
    attribute_table,
)
from data_platform.data_model.silver.volue.commercial_exchange_scheduled import (
    commercial_exchange_scheduled_table,
)
from data_platform.data_model.silver.volue.consumption import (
    consumption_table,
)
from data_platform.data_model.silver.volue.consumption_forecast import (
    consumption_forecast_table,
)
from data_platform.data_model.silver.volue.hydro_inflow import (
    hydro_inflow_table,
)
from data_platform.data_model.silver.volue.hydro_inflow_forecast import (
    hydro_inflow_forecast_table,
)
from data_platform.data_model.silver.volue.hydro_reservoir import (
    hydro_reservoir_table,
)
from data_platform.data_model.silver.volue.hydro_reservoir_forecast import (
    hydro_reservoir_forecast_table,
)
from data_platform.data_model.silver.volue.precipitation import (
    precipitation_table,
)
from data_platform.data_model.silver.volue.precipitation_forecast import (
    precipitation_forecast_table,
)
from data_platform.data_model.silver.volue.price_dayahead import (
    price_dayahead_table,
)
from data_platform.data_model.silver.volue.price_dayahead_forecast import (
    price_dayahead_forecast_table,
)
from data_platform.data_model.silver.volue.price_intraday_auction import (
    price_intraday_auction_table,
)
from data_platform.data_model.silver.volue.price_intraday_continuous import (
    price_intraday_continuous_table,
)
from data_platform.data_model.silver.volue.production import (
    production_table,
)
from data_platform.data_model.silver.volue.production_forecast import (
    production_forecast_table,
)
from data_platform.data_model.silver.volue.residual_load import (
    residual_load_table,
)
from data_platform.data_model.silver.volue.residual_load_forecast import (
    residual_load_forecast_table,
)
from data_platform.data_model.silver.volue.temperature_consumption import (
    temperature_consumption_table,
)
from data_platform.data_model.silver.volue.temperature_consumption_forecast import (
    temperature_consumption_forecast_table,
)

from data_platform.data_model.silver.volue.commercial_exchange_scheduled_forecast import (
    commercial_exchange_scheduled_forecast_table
)

from data_platform.data_model.silver.volue.physical_flow_exchange import (
    physical_flow_exchange_table
)

from data_platform.data_model.silver.volue.power_transfer_distribution_factor_exchange import (
    power_transfer_distribution_factor_exchange_table
)

__all__ = [
    "attribute_table",
    "commercial_exchange_scheduled_table",
    "commercial_exchange_scheduled_forecast_table",
    "consumption_forecast_table",
    "consumption_table",
    "hydro_inflow_forecast_table",
    "hydro_inflow_table",
    "hydro_reservoir_forecast_table",
    "hydro_reservoir_table",
    "precipitation_forecast_table",
    "precipitation_table",
    "price_dayahead_forecast_table",
    "price_dayahead_table",
    "price_intraday_auction_table",
    "price_intraday_continuous_table",
    "production_forecast_table",
    "production_table",
    "residual_load_forecast_table",
    "residual_load_table",
    "temperature_consumption_forecast_table",
    "temperature_consumption_table",
    "physical_flow_exchange_table",
    "power_transfer_distribution_factor_exchange_table",
]
