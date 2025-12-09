"""Silver data model definitions."""
from __future__ import annotations

from data_platform.data_model.silver.nena.consumption import (
    consumption_table,
)
from data_platform.data_model.silver.nena.hydro_inflow import (
    hydro_inflow_table,
)
from data_platform.data_model.silver.nena.hydro_reservoir import (
    hydro_reservoir_table,
)
from data_platform.data_model.silver.nena.nordic_specifics import (
    nordic_specifics_table,
)
from data_platform.data_model.silver.nena.precipitation import (
    precipitation_table,
)
from data_platform.data_model.silver.nena.price_forecast import (
    price_forecast_table,
)
from data_platform.data_model.silver.nena.production import (
    production_table,
)

__all__ = [
    "consumption_table",
    "hydro_inflow_table",
    "hydro_reservoir_table",
    "nordic_specifics_table",
    "precipitation_table",
    "price_forecast_table",
    "production_table",
]
