"""Silver data model definitions."""
from __future__ import annotations

from data_platform.data_model.silver.geocat.attribute import attribute_table
from data_platform.data_model.silver.geocat.production_plant import (
    production_plant_table,
)

__all__ = [
    "attribute_table",
    "production_plant_table",
]
