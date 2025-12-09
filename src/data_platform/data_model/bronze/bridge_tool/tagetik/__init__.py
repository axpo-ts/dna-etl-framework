"""Bronze data model definitions."""

from __future__ import annotations

from data_platform.data_model.bronze.bridge_tool.tagetik.fx_rates import (
    tagetik_fx_rates_model,
)

from data_platform.data_model.bronze.bridge_tool.tagetik.normal_accounts import (
    tagetik_normal_accounts_model,
)


__all__ = ["tagetik_fx_rates_model", "tagetik_normal_accounts_model"]
