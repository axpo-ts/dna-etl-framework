from data_platform.data_model.file_volume_identifier import FileVolumeIdentifier
from data_platform.data_model.table_identifier import TableIdentifier
from data_platform.data_model.unity_catalog_identifier import UnityCatalogObjectType
from data_platform.data_model.view_identifier import ViewIdentifier


def test_view_identifier() -> None:
    view_id = ViewIdentifier("catalog", "schema", "name")

    assert str(view_id) == "catalog.schema.name"
    assert view_id.dna_owned
    assert view_id.type == UnityCatalogObjectType.VIEW


def test_view_identifier_non_dna() -> None:
    view_id = ViewIdentifier("catalog", "schema", "name", False)

    assert str(view_id) == "catalog.schema.name"
    assert not view_id.dna_owned
    assert view_id.type == UnityCatalogObjectType.VIEW


def test_table_identifier() -> None:
    table_id = TableIdentifier("catalog", "schema", "name")

    assert str(table_id) == "catalog.schema.name"
    assert table_id.dna_owned
    assert table_id.type == UnityCatalogObjectType.TABLE


def test_table_identifier_non_dna() -> None:
    table_id = TableIdentifier("catalog", "schema", "name", False)

    assert str(table_id) == "catalog.schema.name"
    assert not table_id.dna_owned
    assert table_id.type == UnityCatalogObjectType.TABLE


def test_volume_identifier() -> None:
    volume_id = FileVolumeIdentifier("catalog", "schema", "name")

    assert str(volume_id) == "/Volumes/catalog/schema/name"
    assert volume_id.dna_owned
    assert volume_id.type == UnityCatalogObjectType.VOLUME


def test_volume_identifier_non_dna() -> None:
    volume_id = FileVolumeIdentifier("catalog", "schema", "name", False)

    assert str(volume_id) == "/Volumes/catalog/schema/name"
    assert not volume_id.dna_owned
    assert volume_id.type == UnityCatalogObjectType.VOLUME
