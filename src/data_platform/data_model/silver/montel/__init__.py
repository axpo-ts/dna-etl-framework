"""Silver data model definitions."""
from __future__ import annotations

from data_platform.data_model.silver.montel.attribute import (
    montel_attribute_table,
)
from data_platform.data_model.silver.montel.price_volume_trade import (
    price_volume_trade_table,
)
from data_platform.data_model.silver.montel.price_volume_trade_statistic import (
    price_volume_trade_statistic_table,
)

__all__ = [
    "montel_attribute_table",
    "price_volume_trade_statistic_table",
    "price_volume_trade_table",
]
