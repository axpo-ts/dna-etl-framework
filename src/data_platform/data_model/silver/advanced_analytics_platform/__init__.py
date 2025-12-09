"""Silver data model definitions."""
from __future__ import annotations

from data_platform.data_model.silver.advanced_analytics_platform.attribute import (
    attribute_table
)
from data_platform.data_model.silver.advanced_analytics_platform.dayahead_volume_flexpool import (
    dayahead_volume_flexpool_table,
)
from data_platform.data_model.silver.advanced_analytics_platform.intraday_price_volume_flexpool import (
    intraday_price_volume_flexpool_table,
)

__all__ = [
    "attribute_table",
    "dayahead_volume_flexpool_table",
    "intraday_price_volume_flexpool_table",
]
