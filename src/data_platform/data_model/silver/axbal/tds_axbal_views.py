from __future__ import annotations

from data_platform.data_model.static_view_model import StaticViewModel
from data_platform.data_model.table_identifier import TableIdentifier
from data_platform.data_model.view_identifier import ViewIdentifier

license_axbal = "DNA_AXBAL"


balhub_silver_view = StaticViewModel(
    identifier=ViewIdentifier(catalog="silver", schema="axbal", name="balhub"),
    license=license_axbal,
    source_table=TableIdentifier(catalog="silver", schema="axbal", name="axbal_balhub", dna_owned=False),
)

balhub_group_silver_view = StaticViewModel(
    identifier=ViewIdentifier(catalog="silver", schema="axbal", name="balhub_group"),
    license=license_axbal,
    source_table=TableIdentifier(catalog="silver", schema="axbal", name="axbal_balhub_group", dna_owned=False),
)

full_supply_unit_silver_view = StaticViewModel(
    identifier=ViewIdentifier(catalog="silver", schema="axbal", name="full_supply_unit"),
    license=license_axbal,
    source_table=TableIdentifier(catalog="silver", schema="axbal", name="axbal_full_supply_unit", dna_owned=False),
)

timeseries_def_silver_view = StaticViewModel(
    identifier=ViewIdentifier(catalog="silver", schema="axbal", name="timeseries_def"),
    license=license_axbal,
    source_table=TableIdentifier(catalog="silver", schema="axbal", name="axbal_timeseries_def", dna_owned=False),
)

wfm_full_supply_unit_contract_map_silver_view = StaticViewModel(
    identifier=ViewIdentifier(
        catalog="silver",
        schema="axbal",
        name="wfm_full_supply_unit_contract_map",
    ),
    license=license_axbal,
    source_table=TableIdentifier(
        catalog="silver",
        schema="axbal",
        name="axbal_wfm_full_supply_unit_contract_map",
        dna_owned=False,
    ),
)

wfm_full_supply_unit_site_map_silver_view = StaticViewModel(
    identifier=ViewIdentifier(
        catalog="silver",
        schema="axbal",
        name="wfm_full_supply_unit_site_map",
    ),
    license=license_axbal,
    source_table=TableIdentifier(
        catalog="silver",
        schema="axbal",
        name="axbal_wfm_full_supply_unit_site_map",
        dna_owned=False,
    ),
)
