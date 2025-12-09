"""Silver data model definitions."""
from __future__ import annotations

from data_platform.data_model.silver.orca_fr.attribute import (
    attribute_table,
)
from data_platform.data_model.silver.orca_fr.flexpool_france_monitoring import (
    flexpool_france_monitoring_table,
)

__all__ = [
    "attribute_table",
    "flexpool_france_monitoring_table",
]
