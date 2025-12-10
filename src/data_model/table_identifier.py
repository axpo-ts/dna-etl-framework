from __future__ import annotations

from dataclasses import dataclass

from data_model.unity_catalog_identifier import (
    UnityCatalogIdentifier,
    UnityCatalogObjectType,
)


@dataclass(slots=True)
class TableIdentifier(UnityCatalogIdentifier):
    """Concrete class for a Table Identifier."""

    type: UnityCatalogObjectType = UnityCatalogObjectType.TABLE
