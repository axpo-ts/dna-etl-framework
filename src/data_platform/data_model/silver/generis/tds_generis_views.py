from __future__ import annotations

from data_platform.data_model.static_view_model import StaticViewModel
from data_platform.data_model.table_identifier import TableIdentifier
from data_platform.data_model.view_identifier import ViewIdentifier

license_generis = "DNA_GENERIS"


generis_reconciliation_reports_silver_view = StaticViewModel(
    identifier=ViewIdentifier(
        catalog="silver",
        schema="generis",
        name="reconciliation_reports",
    ),
    license=license_generis,
    source_table=TableIdentifier(
        catalog="silver",
        schema="generis_reconciliation_report",
        name="generis_reconciliation_reports",
        dna_owned=False,
    ),
)

metering_point_measurements_silver_view = StaticViewModel(
    identifier=ViewIdentifier(
        catalog="silver",
        schema="generis",
        name="metering_point_measurements",
    ),
    license=license_generis,
    source_table=TableIdentifier(
        catalog="silver",
        schema="generis_reconciliation_report",
        name="generis_reconciliation_report_metering_point_measurements",
        dna_owned=False,
    ),
)

metering_points_silver_view = StaticViewModel(
    identifier=ViewIdentifier(
        catalog="silver",
        schema="generis",
        name="metering_points",
    ),
    license=license_generis,
    source_table=TableIdentifier(
        catalog="silver",
        schema="generis_reconciliation_report",
        name="generis_reconciliation_report_metering_points",
        dna_owned=False,
    ),
)
