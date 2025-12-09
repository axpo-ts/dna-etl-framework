from __future__ import annotations

from data_platform.data_model.static_view_model import StaticViewModel
from data_platform.data_model.unity_catalog_identifier import UnityCatalogIdentifier
from data_platform.data_model.view_identifier import ViewIdentifier

license_endur = "DNA_EBX"



preview_book_history_silver_view = StaticViewModel(
    identifier=ViewIdentifier(catalog="silver", schema="ebx", name="book_history"),
    license=license_endur,
    source_table=UnityCatalogIdentifier(catalog="silver", schema="ebx", name="ebx_book_history_view", dna_owned=False),
)

preview_ebx_book_silver_view = StaticViewModel(
    identifier=ViewIdentifier(catalog="silver", schema="ebx", name="book"),
    license=license_endur,
    source_table=UnityCatalogIdentifier(catalog="silver", schema="ebx", name="ebx_book_view", dna_owned=False),
)

preview_business_hierarchy_history_silver_view = StaticViewModel(
    identifier=ViewIdentifier(catalog="silver", schema="ebx", name="business_hierarchy_history"),
    license=license_endur,
    source_table=UnityCatalogIdentifier(catalog="silver", schema="ebx", name="ebx_business_hierarchy_history_view", dna_owned=False),
)

preview_legal_entity_silver_view = StaticViewModel(
    identifier=ViewIdentifier(catalog="silver", schema="ebx", name="legal_entity"),
    license=license_endur,
    source_table=UnityCatalogIdentifier(catalog="silver", schema="ebx", name="ebx_legal_entity_view", dna_owned=False),
)

preview_market_delta_valuation_mapping_silver_view = StaticViewModel(
    identifier=ViewIdentifier(catalog="silver", schema="ebx", name="market_delta_valuation_mapping"),
    license=license_endur,
    source_table=UnityCatalogIdentifier(catalog="silver", schema="ebx", name="ebx_market_delta_valuation_mapping_view", dna_owned=False),
)
