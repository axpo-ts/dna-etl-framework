"""Silver data model definitions."""
from __future__ import annotations

from data_platform.data_model.silver.rte.afrr_energy_activation_invoicing import (
    afrr_energy_activation_invoicing_table,
)
from data_platform.data_model.silver.rte.afrr_fcr_invoicing import (
    afrr_fcr_invoicing_table,
)

__all__ = [
    "afrr_energy_activation_invoicing_table",
    "afrr_fcr_invoicing_table",
]
