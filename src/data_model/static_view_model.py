from dataclasses import dataclass

from data_model.abstract_unity_catalog_data_object_reference import (
    AbstractUnityCatalogDataObjectReference,
)
from data_model.table_identifier import TableIdentifier
from data_model.view_identifier import ViewIdentifier


@dataclass(frozen=True, slots=True)
class StaticViewModel(AbstractUnityCatalogDataObjectReference):
    """A *value object* that fulfils the `AbstractUnityCatalogDataObjectReference`.

    One-liner definitions instead of one class per table:

        bronze_users = StaticViewModel(
            identifier     = ViewIdentifier("lake", "bronze", "users"),
            comment        = "Raw user events coming from the API"
        )

    The dataclass is declared frozen and with slots; that protects the
    definition from accidental mutations and makes each instance lightweight.

    Configuration-driven workflows:
    Because a StaticViewModel is nothing but data, you can read a YAML or
    JSON file and do:

        table = StaticViewModel(**cfg_from_file)

    Without requiring you to create a dedicated subclass for every view.
    Creating a view definition is now as simple as instantiating this
    class with the identifier, schema and (optionally) any extra metadata.
    """

    identifier: ViewIdentifier

    # optional
    comment: str | None = None
    tags: dict[str, str] | None = None
    license: str | None = None
    source_table: TableIdentifier | None = None
