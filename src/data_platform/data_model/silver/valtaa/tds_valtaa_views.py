from __future__ import annotations

from data_platform.data_model.static_view_model import StaticViewModel
from data_platform.data_model.table_identifier import TableIdentifier
from data_platform.data_model.view_identifier import ViewIdentifier

license_valtaa = "DNA_VALTAA"


contract_silver_view = StaticViewModel(
    identifier=ViewIdentifier(catalog="silver", schema="valtaa", name="contract"),
    license=license_valtaa,
    source_table=TableIdentifier(catalog="gold", schema="valtaa", name="valtaa_contract", dna_owned=False),
)

customer_silver_view = StaticViewModel(
    identifier=ViewIdentifier(catalog="silver", schema="valtaa", name="customer"),
    license=license_valtaa,
    source_table=TableIdentifier(catalog="gold", schema="valtaa", name="valtaa_customer", dna_owned=False),
)

gridpoint_pva_config_silver_view = StaticViewModel(
    identifier=ViewIdentifier(catalog="silver", schema="valtaa", name="gridpoint_pva_config"),
    license=license_valtaa,
    source_table=TableIdentifier(
        catalog="gold",
        schema="valtaa",
        name="valtaa_gridpoint_pva_config",
        dna_owned=False,
    ),
)

gridpoint_gold_view = StaticViewModel(
    identifier=ViewIdentifier(catalog="silver", schema="valtaa", name="gridpoint"),
    license=license_valtaa,
    source_table=TableIdentifier(catalog="gold", schema="valtaa", name="valtaa_gridpoint", dna_owned=False),
)

invoice_dp_gold_view = StaticViewModel(
    identifier=ViewIdentifier(catalog="silver", schema="valtaa", name="invoice_dp"),
    license=license_valtaa,
    source_table=TableIdentifier(catalog="gold", schema="valtaa", name="valtaa_invoice_dp", dna_owned=False),
)

invoicelines_silver_view = StaticViewModel(
    identifier=ViewIdentifier(catalog="silver", schema="valtaa", name="invoicelines"),
    license=license_valtaa,
    source_table=TableIdentifier(catalog="gold", schema="valtaa", name="valtaa_invoicelines", dna_owned=False),
)

invoicing_silver_view = StaticViewModel(
    identifier=ViewIdentifier(catalog="silver", schema="valtaa", name="invoicing"),
    license=license_valtaa,
    source_table=TableIdentifier(catalog="gold", schema="valtaa", name="valtaa_invoicing", dna_owned=False),
)
