"""Silver data model definitions."""

from __future__ import annotations

from data_platform.data_model.silver.meteologica.ecmwf_ens_forecast import (
    ecmwf_ens_forecast_table,
)
from data_platform.data_model.silver.meteologica.gfs_forecast import (
    gfs_forecast_table,
)
from data_platform.data_model.silver.meteologica.power_demand import (
    power_demand_table,
)
from data_platform.data_model.silver.meteologica.power_demand_projection import (
    power_demand_projection_table,
)
from data_platform.data_model.silver.meteologica.power_generation import (
    power_generation_table,
)
from data_platform.data_model.silver.meteologica.power_generation_normal import (
    power_generation_normal_table,
)
from data_platform.data_model.silver.meteologica.reanalysis import (
    reanalysis_table,
)

__all__ = [
    "ecmwf_ens_forecast_table",
    "gfs_forecast_table",
    "power_demand_projection_table",
    "power_demand_table",
    "power_generation_normal_table",
    "power_generation_table",
    "reanalysis_table",
]
