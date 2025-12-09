"""Silver data model definitions."""
from __future__ import annotations

from data_platform.data_model.silver.ckw.asset import (
    asset_table,
)
from data_platform.data_model.silver.ckw.attribute import (
    attribute_table,
)
from data_platform.data_model.silver.ckw.contract import (
    contract_table,
)

__all__ = [
    "asset_table",
    "attribute_table",
    "contract_table",
]
