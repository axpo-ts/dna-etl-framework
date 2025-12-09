"""Silver data model definitions."""
from __future__ import annotations

from data_platform.data_model.silver.ebx.customer_group import (
    customer_group_table,
)

from data_platform.data_model.silver.ebx.tds_ebx_views import (
    preview_book_history_silver_view,
    preview_ebx_book_silver_view,
    preview_business_hierarchy_history_silver_view,
    preview_legal_entity_silver_view,
    preview_market_delta_valuation_mapping_silver_view

)

__all__ = [
    "customer_group_table",
    "preview_book_history_silver_view",
    "preview_ebx_book_silver_view",
    "preview_business_hierarchy_history_silver_view",
    "preview_legal_entity_silver_view",
    "preview_market_delta_valuation_mapping_silver_view",
    "preview_business_hierarchy_silver_view",
]
