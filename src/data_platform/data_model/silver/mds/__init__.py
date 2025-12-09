"""Silver data model definitions."""
from __future__ import annotations

from data_platform.data_model.silver.mds.attribute import (
    attribute_table,
)
from data_platform.data_model.silver.mds.curve_fx_forward import (
    curve_fx_forward_table,
)
from data_platform.data_model.silver.mds.factor_cannibalization import (
    factor_cannibalization_table,
)
from data_platform.data_model.silver.mds.factor_discount import (
    factor_discount_table,
)
from data_platform.data_model.silver.mds.precipitation import (
    precipitation_table,
)
from data_platform.data_model.silver.mds.price_afrr_capacity import (
    price_afrr_capacity_table,
)
from data_platform.data_model.silver.mds.price_afrr_energy import (
    price_afrr_energy_table,
)
from data_platform.data_model.silver.mds.price_balancing_energy import (
    price_balancing_energy_table,
)
from data_platform.data_model.silver.mds.price_dayahead import (
    price_dayahead_table,
)
from data_platform.data_model.silver.mds.price_end_of_day_settlement import (
    price_end_of_day_settlement_table,
)
from data_platform.data_model.silver.mds.price_fcr_capacity import (
    price_fcr_capacity_table,
)
from data_platform.data_model.silver.mds.price_weekend import (
    price_weekend_table,
)
from data_platform.data_model.silver.mds.price_bid_ask import (
    price_bid_ask_table,
)
from data_platform.data_model.silver.mds.price_forward_curve import (
    price_forward_curve_table,
)
from data_platform.data_model.silver.mds.production_reanalysis import (
    production_reanalysis_table,
)

__all__ = [
    "attribute_table",
    "curve_fx_forward_table",
    "factor_cannibalization_table",
    "factor_discount_table",
    "precipitation_table",
    "price_afrr_capacity_table",
    "price_afrr_energy_table",
    "price_balancing_energy_table",
    "price_bid_ask_table",
    "price_dayahead_table",
    "price_end_of_day_settlement_table",
    "price_fcr_capacity_table",
    "price_forward_curve_table",
    "price_weekend_table",
    "production_reanalysis_table",
]
