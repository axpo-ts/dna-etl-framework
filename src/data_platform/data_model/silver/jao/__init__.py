"""Silver data model definitions."""
from __future__ import annotations

from data_platform.data_model.silver.jao.bid_transmission_capacity_auction import (
    bid_transmission_capacity_auction_table,
)
from data_platform.data_model.silver.jao.price_volume_transmission_capacity_auction import (
    price_volume_transmission_capacity_auction_table,
)

__all__ = [
    "bid_transmission_capacity_auction_table",
    "price_volume_transmission_capacity_auction_table",
]
