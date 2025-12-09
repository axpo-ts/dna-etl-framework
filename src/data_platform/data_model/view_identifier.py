from dataclasses import dataclass

from data_platform.data_model.unity_catalog_identifier import (
    UnityCatalogIdentifier,
    UnityCatalogObjectType,
)


@dataclass(slots=True)
class ViewIdentifier(UnityCatalogIdentifier):
    """Concrete class for a View Identifier."""

    type: UnityCatalogObjectType = UnityCatalogObjectType.VIEW
