from __future__ import annotations

from data_platform.data_model.static_view_model import StaticViewModel
from data_platform.data_model.unity_catalog_identifier import UnityCatalogIdentifier
from data_platform.data_model.view_identifier import ViewIdentifier

license_endur = "DNA_ENDUR"


ab_tran_history_silver_view = StaticViewModel(
    identifier=ViewIdentifier(catalog="silver", schema="endur", name="ab_tran_history"),
    license=license_endur,
    source_table=UnityCatalogIdentifier(catalog="silver", schema="endur_sod_4ep", name="ab_tran_history", dna_owned=False),
)

ab_tran_info_silver_view = StaticViewModel(
    identifier=ViewIdentifier(catalog="silver", schema="endur", name="ab_tran_info"),
    license=license_endur,
    source_table=UnityCatalogIdentifier(catalog="silver", schema="endur_sod_4ep", name="ab_tran_info", dna_owned=False),
)

asset_type_silver_view = StaticViewModel(
    identifier=ViewIdentifier(catalog="silver", schema="endur", name="asset_type"),
    license=license_endur,
    source_table=UnityCatalogIdentifier(catalog="silver", schema="endur_sod_4ep", name="asset_type", dna_owned=False),
)

buy_sell_silver_view = StaticViewModel(
    identifier=ViewIdentifier(catalog="silver", schema="endur", name="buy_sell"),
    license=license_endur,
    source_table=UnityCatalogIdentifier(catalog="silver", schema="endur_sod_4ep", name="buy_sell", dna_owned=False),
)

cflow_type_silver_view = StaticViewModel(
    identifier=ViewIdentifier(catalog="silver", schema="endur", name="cflow_type"),
    license=license_endur,
    source_table=UnityCatalogIdentifier(catalog="silver", schema="endur_sod_4ep", name="cflow_type", dna_owned=False),
)

country_silver_view = StaticViewModel(
    identifier=ViewIdentifier(catalog="silver", schema="endur", name="country"),
    license=license_endur,
    source_table=UnityCatalogIdentifier(catalog="silver", schema="endur_sod_4ep", name="country", dna_owned=False),
)

currency_silver_view = StaticViewModel(
    identifier=ViewIdentifier(catalog="silver", schema="endur", name="currency"),
    license=license_endur,
    source_table=UnityCatalogIdentifier(catalog="silver", schema="endur_sod_4ep", name="currency", dna_owned=False),
)

delivery_type_silver_view = StaticViewModel(
    identifier=ViewIdentifier(catalog="silver", schema="endur", name="delivery_type"),
    license=license_endur,
    source_table=UnityCatalogIdentifier(
        catalog="silver",
        schema="endur_sod_4ep",
        name="delivery_type",
        dna_owned=False,
    ),
)

functional_group_silver_view = StaticViewModel(
    identifier=ViewIdentifier(catalog="silver", schema="endur", name="functional_group"),
    license=license_endur,
    source_table=UnityCatalogIdentifier(
        catalog="silver",
        schema="endur_sod_4ep",
        name="functional_group",
        dna_owned=False,
    ),
)

header_silver_view = StaticViewModel(
    identifier=ViewIdentifier(catalog="silver", schema="endur", name="header"),
    license=license_endur,
    source_table=UnityCatalogIdentifier(catalog="silver", schema="endur_sod_4ep", name="header", dna_owned=False),
)

idx_group_silver_view = StaticViewModel(
    identifier=ViewIdentifier(catalog="silver", schema="endur", name="idx_group"),
    license=license_endur,
    source_table=UnityCatalogIdentifier(catalog="silver", schema="endur_sod_4ep", name="idx_group", dna_owned=False),
)

idx_unit_silver_view = StaticViewModel(
    identifier=ViewIdentifier(catalog="silver", schema="endur", name="idx_unit"),
    license=license_endur,
    source_table=UnityCatalogIdentifier(catalog="silver", schema="endur_sod_4ep", name="idx_unit", dna_owned=False),
)

ins_class_silver_view = StaticViewModel(
    identifier=ViewIdentifier(catalog="silver", schema="endur", name="ins_class"),
    license=license_endur,
    source_table=UnityCatalogIdentifier(catalog="silver", schema="endur_sod_4ep", name="ins_class", dna_owned=False),
)

ins_sub_type_silver_view = StaticViewModel(
    identifier=ViewIdentifier(catalog="silver", schema="endur", name="ins_sub_type"),
    license=license_endur,
    source_table=UnityCatalogIdentifier(
        catalog="silver",
        schema="endur_sod_4ep",
        name="ins_sub_type",
        dna_owned=False,
    ),
)

instruments_silver_view = StaticViewModel(
    identifier=ViewIdentifier(catalog="silver", schema="endur", name="instruments"),
    license=license_endur,
    source_table=UnityCatalogIdentifier(catalog="silver", schema="endur_sod_4ep", name="instruments", dna_owned=False),
)

party_silver_view = StaticViewModel(
    identifier=ViewIdentifier(catalog="silver", schema="endur", name="party"),
    license=license_endur,
    source_table=UnityCatalogIdentifier(catalog="silver", schema="endur_sod_4ep", name="party", dna_owned=False),
)

party_address_silver_view = StaticViewModel(
    identifier=ViewIdentifier(catalog="silver", schema="endur", name="party_address"),
    license=license_endur,
    source_table=UnityCatalogIdentifier(
        catalog="silver",
        schema="endur_sod_4ep",
        name="party_address",
        dna_owned=False,
    ),
)

pers_license_types_link_silver_view = StaticViewModel(
    identifier=ViewIdentifier(catalog="silver", schema="endur", name="pers_license_types_link"),
    license=license_endur,
    source_table=UnityCatalogIdentifier(
        catalog="silver",
        schema="endur_sod_4ep",
        name="pers_license_types_link",
        dna_owned=False,
    ),
)

personnel_silver_view = StaticViewModel(
    identifier=ViewIdentifier(catalog="silver", schema="endur", name="personnel"),
    license=license_endur,
    source_table=UnityCatalogIdentifier(catalog="silver", schema="endur_sod_4ep", name="personnel", dna_owned=False),
)

personnel_functional_group_silver_view = StaticViewModel(
    identifier=ViewIdentifier(catalog="silver", schema="endur", name="personnel_functional_group"),
    license=license_endur,
    source_table=UnityCatalogIdentifier(
        catalog="silver",
        schema="endur_sod_4ep",
        name="personnel_functional_group",
        dna_owned=False,
    ),
)

portfolio_silver_view = StaticViewModel(
    identifier=ViewIdentifier(catalog="silver", schema="endur", name="portfolio"),
    license=license_endur,
    source_table=UnityCatalogIdentifier(catalog="silver", schema="endur_sod_4ep", name="portfolio", dna_owned=False),
)

pricing_model_silver_view = StaticViewModel(
    identifier=ViewIdentifier(catalog="silver", schema="endur", name="pricing_model"),
    license=license_endur,
    source_table=UnityCatalogIdentifier(catalog="silver", schema="endur_sod_4ep", name="pricing_model", dna_owned=False),
)

stldoc_details_hist_silver_view = StaticViewModel(
    identifier=ViewIdentifier(catalog="silver", schema="endur", name="stldoc_details_hist"),
    license=license_endur,
    source_table=UnityCatalogIdentifier(
        catalog="silver",
        schema="endur_sod_4ep",
        name="stldoc_details_hist",
        dna_owned=False,
    ),
)

stldoc_document_status_silver_view = StaticViewModel(
    identifier=ViewIdentifier(catalog="silver", schema="endur", name="stldoc_document_status"),
    license=license_endur,
    source_table=UnityCatalogIdentifier(
        catalog="silver",
        schema="endur_sod_4ep",
        name="stldoc_document_status",
        dna_owned=False,
    ),
)

stldoc_header_hist_silver_view = StaticViewModel(
    identifier=ViewIdentifier(catalog="silver", schema="endur", name="stldoc_header_hist"),
    license=license_endur,
    source_table=UnityCatalogIdentifier(
        catalog="silver",
        schema="endur_sod_4ep",
        name="stldoc_header_hist",
        dna_owned=False,
    ),
)

toolsets_silver_view = StaticViewModel(
    identifier=ViewIdentifier(catalog="silver", schema="endur", name="toolsets"),
    license=license_endur,
    source_table=UnityCatalogIdentifier(catalog="silver", schema="endur_sod_4ep", name="toolsets", dna_owned=False),
)

trade_maint_type_silver_view = StaticViewModel(
    identifier=ViewIdentifier(catalog="silver", schema="endur", name="trade_maint_type"),
    license=license_endur,
    source_table=UnityCatalogIdentifier(
        catalog="silver",
        schema="endur_sod_4ep",
        name="trade_maint_type",
        dna_owned=False,
    ),
)

tran_info_types_silver_view = StaticViewModel(
    identifier=ViewIdentifier(catalog="silver", schema="endur", name="tran_info_types"),
    license=license_endur,
    source_table=UnityCatalogIdentifier(
        catalog="silver",
        schema="endur_sod_4ep",
        name="tran_info_types",
        dna_owned=False,
    ),
)

trans_status_silver_view = StaticViewModel(
    identifier=ViewIdentifier(catalog="silver", schema="endur", name="trans_status"),
    license=license_endur,
    source_table=UnityCatalogIdentifier(catalog="silver", schema="endur_sod_4ep", name="trans_status", dna_owned=False),
)

trans_type_silver_view = StaticViewModel(
    identifier=ViewIdentifier(catalog="silver", schema="endur", name="trans_type"),
    license=license_endur,
    source_table=UnityCatalogIdentifier(catalog="silver", schema="endur_sod_4ep", name="trans_type", dna_owned=False),
)

unit_type_silver_view = StaticViewModel(
    identifier=ViewIdentifier(catalog="silver", schema="endur", name="unit_type"),
    license=license_endur,
    source_table=UnityCatalogIdentifier(catalog="silver", schema="endur_sod_4ep", name="unit_type", dna_owned=False),
)

user_group_to_user_silver_view = StaticViewModel(
    identifier=ViewIdentifier(catalog="silver", schema="endur", name="user_group_to_user"),
    license=license_endur,
    source_table=UnityCatalogIdentifier(
        catalog="silver",
        schema="endur_sod_4ep",
        name="user_group_to_user",
        dna_owned=False,
    ),
)

endur_deals_raw_transaction_latest_view = StaticViewModel(
    identifier=ViewIdentifier(catalog="silver", schema="endur", name="deals_raw_transaction_latest"),
    license=license_endur,
    source_table=UnityCatalogIdentifier(
        catalog="silver",
        schema="endur_deals",
        name="endur_raw_transaction_latest",
        dna_owned=False,
    ),
)

endur_deals_raw_transaction_view = StaticViewModel(
    identifier=ViewIdentifier(catalog="silver", schema="endur", name="deals_raw_transaction"),
    license=license_endur,
    source_table=UnityCatalogIdentifier(
        catalog="silver",
        schema="endur_deals",
        name="endur_raw_transaction_view",
        dna_owned=False,
    ),
)

preview_endur_indexes_silver_view = StaticViewModel(
    identifier=ViewIdentifier(catalog="silver", schema="endur", name="endur_indexes"),
    license=license_endur,
    source_table=UnityCatalogIdentifier(
        catalog="silver",
        schema="endur_price_domain",
        name="endur_indexes",
        dna_owned=False,
    ),
)

preview_endur_active_portfolios_view_silver_view = StaticViewModel(
    identifier=ViewIdentifier(catalog="silver", schema="endur", name="endur_active_portfolios_view"),
    license=license_endur,
    source_table=UnityCatalogIdentifier(
        catalog="silver",
        schema="endur_reference_domain",
        name="endur_active_portfolios_view",
        dna_owned=False,
    ),
)

preview_endur_instrument_types_silver_view = StaticViewModel(
    identifier=ViewIdentifier(catalog="silver", schema="endur", name="endur_instrument_types"),
    license=license_endur,
    source_table=UnityCatalogIdentifier(
        catalog="silver",
        schema="endur_reference_domain",
        name="endur_instrument_types",
        dna_owned=False,
    ),
)

preview_endur_parties_silver_view = StaticViewModel(
    identifier=ViewIdentifier(catalog="silver", schema="endur", name="endur_parties"),
    license=license_endur,
    source_table=UnityCatalogIdentifier(
        catalog="silver",
        schema="endur_reference_domain",
        name="endur_parties",
        dna_owned=False,
    ),
)

preview_endur_portfolios_silver_view = StaticViewModel(
    identifier=ViewIdentifier(catalog="silver", schema="endur", name="endur_portfolios"),
    license=license_endur,
    source_table=UnityCatalogIdentifier(
        catalog="silver",
        schema="endur_reference_domain",
        name="endur_portfolios",
        dna_owned=False,
    ),
)

preview_endur_power_locations_silver_view = StaticViewModel(
    identifier=ViewIdentifier(catalog="silver", schema="endur", name="endur_power_locations"),
    license=license_endur,
    source_table=UnityCatalogIdentifier(
        catalog="silver",
        schema="endur_reference_domain",
        name="endur_power_locations",
        dna_owned=False,
    ),
)

preview_endur_transaction_status_silver_view = StaticViewModel(
    identifier=ViewIdentifier(catalog="silver", schema="endur", name="endur_transaction_status"),
    license=license_endur,
    source_table=UnityCatalogIdentifier(
        catalog="silver",
        schema="endur_reference_domain",
        name="endur_transaction_status",
        dna_owned=False,
    ),
)

preview_endur_volume_types_silver_view = StaticViewModel(
    identifier=ViewIdentifier(catalog="silver", schema="endur", name="endur_volume_types"),
    license=license_endur,
    source_table=UnityCatalogIdentifier(
        catalog="silver",
        schema="endur_reference_domain",
        name="endur_volume_types",
        dna_owned=False,
    ),
)

preview_endur_dwh_delta_gamma_result_view_silver_view = StaticViewModel(
    identifier=ViewIdentifier(catalog="silver", schema="endur", name="dwh_delta_gamma_result_view"),
    license=license_endur,
    source_table=UnityCatalogIdentifier(
        catalog="silver",
        schema="endur_simulation_domain",
        name="endur_dwh_delta_gamma_result_view",
        dna_owned=False,
    ),
)

preview_endur_tran_gpt_delta_by_leg_view_silver_view = StaticViewModel(
    identifier=ViewIdentifier(catalog="silver", schema="endur", name="tran_gpt_delta_by_leg_view"),
    license=license_endur,
    source_table=UnityCatalogIdentifier(
        catalog="silver",
        schema="endur_simulation_domain",
        name="endur_tran_gpt_delta_by_leg_view",
        dna_owned=False,
    ),
)

preview_endur_dim_periods_silver_view = StaticViewModel(
    identifier=ViewIdentifier(catalog="silver", schema="endur", name="dim_periods"),
    license=license_endur,
    source_table=UnityCatalogIdentifier(
        catalog="silver",
        schema="endur_volume_domain",
        name="endur_dim_periods",
        dna_owned=False,
    ),
)

preview_endur_physical_power_volume_silver_view = StaticViewModel(
    identifier=ViewIdentifier(catalog="silver", schema="endur", name="physical_power_volume"),
    license=license_endur,
    source_table=UnityCatalogIdentifier(
        catalog="silver",
        schema="endur_volume_domain",
        name="endur_physical_power_volume",
        dna_owned=False,
    ),
)
