"""Silver data model definitions."""

from __future__ import annotations

from data_platform.data_model.silver.generis.tds_generis_views import (
    generis_reconciliation_reports_silver_view,
    metering_point_measurements_silver_view,
    metering_points_silver_view,
)

__all__ = [
    "generis_reconciliation_reports_silver_view",
    "metering_point_measurements_silver_view",
    "metering_points_silver_view",
]
