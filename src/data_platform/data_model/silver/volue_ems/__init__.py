"""Silver data model definitions."""
from __future__ import annotations

from data_platform.data_model.silver.volue_ems.invoicing_price import (
    invoicing_price_table,
)
from data_platform.data_model.silver.volue_ems.invoicing_volume import (
    invoicing_volume_table,
)

__all__ = [
    "invoicing_price_table",
    "invoicing_volume_table",
]
