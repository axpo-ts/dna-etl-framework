from abc import ABC, abstractmethod

from data_platform.data_model.unity_catalog_identifier import UnityCatalogIdentifier


class AbstractUnityCatalogDataObjectReference(ABC):
    """A minimal way to reference a Unity Catalog Object.

    Sub-classes *must* supply:
        • the identifier (catalog / schema / name)

    Everything else is optional and therefore provided with defaults.
    """

    @property
    @abstractmethod
    def identifier(self) -> UnityCatalogIdentifier:
        """catalog.schema.table wrapped in one object."""
        ...

    @property
    def comment(self) -> str | None:
        """Returns the description for the table."""
        return None

    @property
    def tags(self) -> dict[str, str] | None:
        """Tags for the table."""
        return None

    @property
    def license(self) -> str:
        """License identifier for the table."""
        return None

    @property
    def full_name(self) -> str:
        """Returns `catalog.schema.table` (or `schema.table` if catalog is None)."""
        return str(self.identifier)

    def __str__(self) -> str:
        """Returns a human-readable string representation of the table."""
        return f"<Table {self.full_name}>"
