from __future__ import annotations

from data_platform.data_model.static_view_model import StaticViewModel
from data_platform.data_model.table_identifier import TableIdentifier
from data_platform.data_model.view_identifier import ViewIdentifier

license_workflowmanager = "DNA_WORKFLOWMANAGER"


business_unit_fee_silver_view = StaticViewModel(
    identifier=ViewIdentifier(catalog="silver", schema="workflowmanager", name="business_unit_fee"),
    license=license_workflowmanager,
    source_table=TableIdentifier(
        catalog="silver",
        schema="workflowmanager",
        name="wfm_business_unit_fee",
        dna_owned=False,
    ),
)

contract_silver_view = StaticViewModel(
    identifier=ViewIdentifier(catalog="silver", schema="workflowmanager", name="contract"),
    license=license_workflowmanager,
    source_table=TableIdentifier(catalog="silver", schema="workflowmanager", name="wfm_contract", dna_owned=False),
)

contract_approval_silver_view = StaticViewModel(
    identifier=ViewIdentifier(catalog="silver", schema="workflowmanager", name="contract_approval"),
    license=license_workflowmanager,
    source_table=TableIdentifier(
        catalog="silver",
        schema="workflowmanager",
        name="wfm_contract_approval",
        dna_owned=False,
    ),
)

contract_approval_history_silver_view = StaticViewModel(
    identifier=ViewIdentifier(catalog="silver", schema="workflowmanager", name="contract_approval_history"),
    license=license_workflowmanager,
    source_table=TableIdentifier(
        catalog="silver",
        schema="workflowmanager",
        name="wfm_contract_approval_history",
        dna_owned=False,
    ),
)

contract_associated_legal_entity_silver_view = StaticViewModel(
    identifier=ViewIdentifier(
        catalog="silver",
        schema="workflowmanager",
        name="contract_associated_legal_entity",
    ),
    license=license_workflowmanager,
    source_table=TableIdentifier(
        catalog="silver",
        schema="workflowmanager",
        name="wfm_contract_associated_legal_entity",
        dna_owned=False,
    ),
)

contract_associated_site_silver_view = StaticViewModel(
    identifier=ViewIdentifier(catalog="silver", schema="workflowmanager", name="contract_associated_site"),
    license=license_workflowmanager,
    source_table=TableIdentifier(
        catalog="silver",
        schema="workflowmanager",
        name="wfm_contract_associated_site",
        dna_owned=False,
    ),
)

contract_attachment_silver_view = StaticViewModel(
    identifier=ViewIdentifier(catalog="silver", schema="workflowmanager", name="contract_attachment"),
    license=license_workflowmanager,
    source_table=TableIdentifier(
        catalog="silver",
        schema="workflowmanager",
        name="wfm_contract_attachment",
        dna_owned=False,
    ),
)

contract_endur_booker_silver_view = StaticViewModel(
    identifier=ViewIdentifier(catalog="silver", schema="workflowmanager", name="contract_endur_booker"),
    license=license_workflowmanager,
    source_table=TableIdentifier(
        catalog="silver",
        schema="workflowmanager",
        name="wfm_contract_endur_booker",
        dna_owned=False,
    ),
)

contract_fee_silver_view = StaticViewModel(
    identifier=ViewIdentifier(catalog="silver", schema="workflowmanager", name="contract_fee"),
    license=license_workflowmanager,
    source_table=TableIdentifier(catalog="silver", schema="workflowmanager", name="wfm_contract_fee", dna_owned=False),
)

contract_onboarding_readiness_task_silver_view = StaticViewModel(
    identifier=ViewIdentifier(
        catalog="silver",
        schema="workflowmanager",
        name="contract_onboarding_readiness_task",
    ),
    license=license_workflowmanager,
    source_table=TableIdentifier(
        catalog="silver",
        schema="workflowmanager",
        name="wfm_contract_onboarding_readiness_task",
        dna_owned=False,
    ),
)

renewable_asset_coordinates_silver_view = StaticViewModel(
    identifier=ViewIdentifier(catalog="silver", schema="workflowmanager", name="renewable_asset_coordinates"),
    license=license_workflowmanager,
    source_table=TableIdentifier(
        catalog="silver",
        schema="workflowmanager",
        name="wfm_renewable_asset_coordinates",
        dna_owned=False,
    ),
)

site_silver_view = StaticViewModel(
    identifier=ViewIdentifier(catalog="silver", schema="workflowmanager", name="site"),
    license=license_workflowmanager,
    source_table=TableIdentifier(catalog="silver", schema="workflowmanager", name="wfm_site", dna_owned=False),
)

