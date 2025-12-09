from __future__ import annotations

from dataclasses import dataclass
from enum import Enum


class UnityCatalogObjectType(Enum):
    """Models the types that can sit under a schema in UC."""

    TABLE = "table"
    VIEW = "view"
    VOLUME = "volume"
    FUNCTION = "function"
    UNKNOWN = "unknown"


@dataclass(slots=True)
class UnityCatalogIdentifier:
    """A simple value object that represents an identifier for a unity catalog object.

    It is a combination of catalog, schema, and name.

    Attributes:
        type: UnityCatalogObjectType
            Table/View/Volume/Function
        catalog: str | None
            The catalog name, or None if not applicable.
        schema: str
            The schema name.
        name: str
            The table name.
        dna_owned: bool
            Indicates the Table is hosted in a DNA workspace; defaults to true.
    """

    catalog: str | None
    schema: str
    name: str
    dna_owned: bool = True
    type: UnityCatalogObjectType = UnityCatalogObjectType.UNKNOWN

    def __str__(self) -> str:
        """Returns the identifier as `catalog.schema.name`."""
        # filter(None, …) removes the catalog part when it is None
        return ".".join(filter(None, (self.catalog, self.schema, self.name)))
