from data_platform.tasks.reader.config.dataclasses import SimpleReaderConfig
from data_platform.tasks.utils import CatalogUtilities


def test_build_table_name_with_catalog_name() -> None:
    assert "`catalog`.`schema`.`table`" == CatalogUtilities.build_table_name("catalog", "schema", "table")


def test_build_table_name_none_catalog_name() -> None:
    assert "`schema`.`table`" == CatalogUtilities.build_table_name(None, "schema", "table")


def test_build_table_name_from_conf_with_catalog_name() -> None:
    _conf = SimpleReaderConfig(
        task_name="task",
        catalog_name="catalog",
        schema_name="schema",
        table_name="table",
        _format="format",
        df_key="key",
        df_namespace="namespace",
    )
    assert "`catalog`.`schema`.`table`" == CatalogUtilities.build_table_name_from_conf(_conf)


def test_build_table_name_from_conf_none_catalog_name() -> None:
    _conf = SimpleReaderConfig(
        task_name="task",
        schema_name="schema",
        table_name="table",
        _format="format",
        df_key="key",
        df_namespace="namespace",
    )
    assert "`schema`.`table`" == CatalogUtilities.build_table_name_from_conf(_conf)
