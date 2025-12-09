"""Silver data model definitions."""
from __future__ import annotations

from data_platform.data_model.silver.elia.power_afrr_prices import (
    elia_price_afrr_capacity_table,
    elia_price_afrr_energy_table,
)

__all__ = [
    "elia_price_afrr_capacity_table",
    "elia_price_afrr_energy_table",
]
