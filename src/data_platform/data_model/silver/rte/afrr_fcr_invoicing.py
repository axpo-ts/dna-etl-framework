from __future__ import annotations

from pyspark.sql.types import DoubleType, StringType, StructField, StructType

from data_platform.data_model import StaticTableModel, TableIdentifier

from .rte_common import RTE_COMMON_TAGS, RTE_LICENSE

afrr_fcr_invoicing_table = StaticTableModel(
    identifier=TableIdentifier("silver", "rte", "afrr_fcr_invoicing"),
    schema=StructType(
        [
            StructField("month", StringType(), True),
            StructField("eic_code", StringType(), True),
            StructField("version", StringType(), True),
            StructField("resolution", StringType(), True),
            StructField("period_start", StringType(), True),
            StructField("period_end", StringType(), True),
            StructField("spot_price", DoubleType(), True),
            StructField("a_coeff", DoubleType(), True),
            StructField("s_coeff", DoubleType(), True),
            # FCR fields
            StructField("fcr_energy_volume_up", DoubleType(), True),
            StructField("fcr_energy_volume_down", DoubleType(), True),
            StructField("fcr_energy_remuneration_up", DoubleType(), True),
            StructField("fcr_energy_remuneration_down", DoubleType(), True),
            # FCR reserve balance fields
            StructField("fcr_reserve_sum_ner_up", DoubleType(), True),
            StructField("fcr_reserve_sum_ner_down", DoubleType(), True),
            StructField("fcr_reserve_sum_paj_up", DoubleType(), True),
            StructField("fcr_reserve_sum_paj_down", DoubleType(), True),
            StructField("fcr_reserve_sum_pajhf_up", DoubleType(), True),
            StructField("fcr_reserve_sum_pajhf_down", DoubleType(), True),
            StructField("fcr_reserve_sum_pm_indemnity_up", DoubleType(), True),
            StructField("fcr_reserve_sum_pm_indemnity_down", DoubleType(), True),
            StructField("fcr_reserve_balance_paj_up", DoubleType(), True),
            StructField("fcr_reserve_balance_paj_down", DoubleType(), True),
            StructField("fcr_reserve_balance_pajhf_up", DoubleType(), True),
            StructField("fcr_reserve_balance_pajhf_down", DoubleType(), True),
            StructField("fcr_reserve_balance_pm_indemnity_up", DoubleType(), True),
            StructField("fcr_reserve_balance_pm_indemnity_down", DoubleType(), True),
            # FCR indemnities fields
            StructField("fcr_indemnities_elementary_up", DoubleType(), True),
            StructField("fcr_indemnities_elementary_down", DoubleType(), True),
            StructField("fcr_indemnities_pa_up", DoubleType(), True),
            StructField("fcr_indemnities_pa_down", DoubleType(), True),
            StructField("fcr_indemnities_pa", DoubleType(), True),
            StructField("fcr_indemnities_volume_reserve_up", DoubleType(), True),
            StructField("fcr_indemnities_volume_reserve_down", DoubleType(), True),
            StructField("fcr_indemnities_pm_up", DoubleType(), True),
            StructField("fcr_indemnities_pm_down", DoubleType(), True),
            # FCR capa reserve remuneration fields
            StructField("fcr_capa_engagement", DoubleType(), True),
            StructField("fcr_capa_price", DoubleType(), True),
            StructField("fcr_capa_contractualisation_mode", StringType(), True),
            StructField("fcr_capa_remuneration", DoubleType(), True),
            # aFRR fields
            StructField("afrr_energy_volume_up", DoubleType(), True),
            StructField("afrr_energy_volume_down", DoubleType(), True),
            StructField("afrr_energy_remuneration_up", DoubleType(), True),
            StructField("afrr_energy_remuneration_down", DoubleType(), True),
            # aFRR reserve balance fields
            StructField("afrr_reserve_sum_ner_up", DoubleType(), True),
            StructField("afrr_reserve_sum_ner_down", DoubleType(), True),
            StructField("afrr_reserve_sum_paj_up", DoubleType(), True),
            StructField("afrr_reserve_sum_paj_down", DoubleType(), True),
            StructField("afrr_reserve_sum_pajhf_up", DoubleType(), True),
            StructField("afrr_reserve_sum_pajhf_down", DoubleType(), True),
            StructField("afrr_reserve_sum_pm_indemnity_up", DoubleType(), True),
            StructField("afrr_reserve_sum_pm_indemnity_down", DoubleType(), True),
            StructField("afrr_reserve_balance_paj_up", DoubleType(), True),
            StructField("afrr_reserve_balance_paj_down", DoubleType(), True),
            StructField("afrr_reserve_balance_pajhf_up", DoubleType(), True),
            StructField("afrr_reserve_balance_pajhf_down", DoubleType(), True),
            StructField("afrr_reserve_balance_pm_indemnity_up", DoubleType(), True),
            StructField("afrr_reserve_balance_pm_indemnity_down", DoubleType(), True),
            # aFRR indemnities fields
            StructField("afrr_indemnities_elementary_up", DoubleType(), True),
            StructField("afrr_indemnities_elementary_down", DoubleType(), True),
            StructField("afrr_indemnities_pa_up", DoubleType(), True),
            StructField("afrr_indemnities_pa_down", DoubleType(), True),
            StructField("afrr_indemnities_pa", DoubleType(), True),
            StructField("afrr_indemnities_volume_up", DoubleType(), True),
            StructField("afrr_indemnities_volume_down", DoubleType(), True),
            StructField("afrr_indemnities_pm_up", DoubleType(), True),
            StructField("afrr_indemnities_pm_down", DoubleType(), True),
            # aFRR capa reserve remuneration fields
            StructField("afrr_capa_engagement_up", DoubleType(), True),
            StructField("afrr_capa_engagement_down", DoubleType(), True),
            StructField("afrr_capa_price_up", DoubleType(), True),
            StructField("afrr_capa_price_down", DoubleType(), True),
            StructField("afrr_capa_contractualisation_mode", StringType(), True),
            StructField("afrr_capa_remuneration_up", DoubleType(), True),
            StructField("afrr_capa_remuneration_down", DoubleType(), True),
        ]
    ),
    comment="Combined invoicing data for automatic Frequency Restoration Reserve (aFRR) and Frequency Containment Reserve (FCR) from RTE (French TSO)",
    license=RTE_LICENSE,
    tags=RTE_COMMON_TAGS,
    primary_keys=("month", "eic_code", "version", "resolution", "period_start", "period_end"),
)
