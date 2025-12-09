"""Silver data model definitions."""
from __future__ import annotations

from data_platform.data_model.silver.valtaa.tds_valtaa_views import (
    contract_silver_view,
    customer_silver_view,
    gridpoint_gold_view,
    gridpoint_pva_config_silver_view,
    invoice_dp_gold_view,
    invoicelines_silver_view,
    invoicing_silver_view,
)

__all__ = [
    "contract_silver_view",
    "customer_silver_view",
    "gridpoint_gold_view",
    "gridpoint_pva_config_silver_view",
    "invoice_dp_gold_view",
    "invoicelines_silver_view",
    "invoicing_silver_view",
]
