from __future__ import annotations

from data_platform.data_model.static_view_model import StaticViewModel
from data_platform.data_model.table_identifier import TableIdentifier
from data_platform.data_model.view_identifier import ViewIdentifier

license_aaru = "DNA_AARU"


aggregate_silver_view = StaticViewModel(
    identifier=ViewIdentifier(catalog="silver", schema="aaru", name="aggregate"),
    license=license_aaru,
    source_table=TableIdentifier(catalog="silver", schema="aaru", name="aaru_aggregate", dna_owned=False),
)

correctionfactor_silver_view = StaticViewModel(
    identifier=ViewIdentifier(catalog="silver", schema="aaru", name="correctionfactor"),
    license=license_aaru,
    source_table=TableIdentifier(catalog="silver", schema="aaru", name="aaru_correctionfactor", dna_owned=False),
)

gates_silver_view = StaticViewModel(
    identifier=ViewIdentifier(catalog="silver", schema="aaru", name="gates"),
    license=license_aaru,
    source_table=TableIdentifier(catalog="silver", schema="aaru", name="aaru_gates", dna_owned=False),
)

generis_silver_view = StaticViewModel(
    identifier=ViewIdentifier(catalog="silver", schema="aaru", name="generis"),
    license=license_aaru,
    source_table=TableIdentifier(catalog="silver", schema="aaru", name="aaru_generis", dna_owned=False),
)

gridarea_silver_view = StaticViewModel(
    identifier=ViewIdentifier(catalog="silver", schema="aaru", name="gridarea"),
    license=license_aaru,
    source_table=TableIdentifier(catalog="silver", schema="aaru", name="aaru_gridarea", dna_owned=False),
)

meteo_fc_silver_view = StaticViewModel(
    identifier=ViewIdentifier(catalog="silver", schema="aaru", name="meteo_fc"),
    license=license_aaru,
    source_table=TableIdentifier(catalog="silver", schema="aaru", name="aaru_meteo_fc", dna_owned=False),
)

meteo_meas_silver_view = StaticViewModel(
    identifier=ViewIdentifier(catalog="silver", schema="aaru", name="meteo_meas"),
    license=license_aaru,
    source_table=TableIdentifier(catalog="silver", schema="aaru", name="aaru_meteo_meas", dna_owned=False),
)

meteoswisslocations_silver_view = StaticViewModel(
    identifier=ViewIdentifier(catalog="silver", schema="aaru", name="meteoswisslocations"),
    license=license_aaru,
    source_table=TableIdentifier(
        catalog="silver",
        schema="aaru",
        name="aaru_meteoswisslocations",
        dna_owned=False,
    ),
)

meterpoint_silver_view = StaticViewModel(
    identifier=ViewIdentifier(catalog="silver", schema="aaru", name="meterpoint"),
    license=license_aaru,
    source_table=TableIdentifier(catalog="silver", schema="aaru", name="aaru_meterpoint", dna_owned=False),
)

nlaggregate_silver_view = StaticViewModel(
    identifier=ViewIdentifier(catalog="silver", schema="aaru", name="nlaggregate"),
    license=license_aaru,
    source_table=TableIdentifier(catalog="silver", schema="aaru", name="aaru_nlaggregate", dna_owned=False),
)

nlprofile_silver_view = StaticViewModel(
    identifier=ViewIdentifier(catalog="silver", schema="aaru", name="nlprofile"),
    license=license_aaru,
    source_table=TableIdentifier(catalog="silver", schema="aaru", name="aaru_nlprofile", dna_owned=False),
)

profile_silver_view = StaticViewModel(
    identifier=ViewIdentifier(catalog="silver", schema="aaru", name="profile"),
    license=license_aaru,
    source_table=TableIdentifier(catalog="silver", schema="aaru", name="aaru_profile", dna_owned=False),
)

profile_factor_silver_view = StaticViewModel(
    identifier=ViewIdentifier(catalog="silver", schema="aaru", name="profile_factor"),
    license=license_aaru,
    source_table=TableIdentifier(catalog="silver", schema="aaru", name="aaru_profile_factor", dna_owned=False),
)

prototype_silver_view = StaticViewModel(
    identifier=ViewIdentifier(catalog="silver", schema="aaru", name="prototype"),
    license=license_aaru,
    source_table=TableIdentifier(catalog="silver", schema="aaru", name="aaru_prototype", dna_owned=False),
)

quality_silver_view = StaticViewModel(
    identifier=ViewIdentifier(catalog="silver", schema="aaru", name="quality"),
    license=license_aaru,
    source_table=TableIdentifier(catalog="silver", schema="aaru", name="aaru_quality", dna_owned=False),
)

register_silver_view = StaticViewModel(
    identifier=ViewIdentifier(catalog="silver", schema="aaru", name="register"),
    license=license_aaru,
    source_table=TableIdentifier(catalog="silver", schema="aaru", name="aaru_register", dna_owned=False),
)

timeseries_silver_view = StaticViewModel(
    identifier=ViewIdentifier(catalog="silver", schema="aaru", name="timeseries"),
    license=license_aaru,
    source_table=TableIdentifier(catalog="silver", schema="aaru", name="aaru_timeseries", dna_owned=False),
)

unit_silver_view = StaticViewModel(
    identifier=ViewIdentifier(catalog="silver", schema="aaru", name="unit"),
    license=license_aaru,
    source_table=TableIdentifier(catalog="silver", schema="aaru", name="aaru_unit", dna_owned=False),
)

values_silver_view = StaticViewModel(
    identifier=ViewIdentifier(catalog="silver", schema="aaru", name="values"),
    license=license_aaru,
    source_table=TableIdentifier(catalog="silver", schema="aaru", name="aaru_values", dna_owned=False),
)

versionrule_silver_view = StaticViewModel(
    identifier=ViewIdentifier(catalog="silver", schema="aaru", name="versionrule"),
    license=license_aaru,
    source_table=TableIdentifier(catalog="silver", schema="aaru", name="aaru_versionrule", dna_owned=False),
)
