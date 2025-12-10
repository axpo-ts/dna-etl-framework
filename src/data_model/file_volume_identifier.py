from __future__ import annotations

from dataclasses import dataclass

from data_model.unity_catalog_identifier import (
    UnityCatalogIdentifier,
    UnityCatalogObjectType,
)


@dataclass(slots=True)
class FileVolumeIdentifier(UnityCatalogIdentifier):
    """Represents a Unity Catalog volume identifier."""

    type: UnityCatalogObjectType = UnityCatalogObjectType.VOLUME
    file_path: str | None = None

    def __str__(self) -> str:
        """Returns the volume identifier as `/Volumes/catalog/schema/volume_name/file_path`."""
        base = f"/Volumes/{self.catalog}/{self.schema}/{self.name}"
        if self.file_path is None:
            return base.rstrip("/")
        else:
            return f"{base}/{self.file_path}".rstrip("/")
