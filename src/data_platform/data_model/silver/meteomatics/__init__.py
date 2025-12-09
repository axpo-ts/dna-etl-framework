"""Silver data model definitions."""

from __future__ import annotations

from data_platform.data_model.silver.meteomatics.attribute import (
    attribute_table,
)
from data_platform.data_model.silver.meteomatics.cloud_cover_forecast import (
    meteomatics_cloud_cover_forecast_table,
)
from data_platform.data_model.silver.meteomatics.geopotential_height import (
    meteomatics_geopotential_height_table,
)
from data_platform.data_model.silver.meteomatics.humidity_forecast import (
    meteomatics_humidity_forecast_table,
)
from data_platform.data_model.silver.meteomatics.precipitation import (
    meteomatics_precipitation_table,
)
from data_platform.data_model.silver.meteomatics.precipitation_forecast import (
    meteomatics_precipitation_forecast_table,
)
from data_platform.data_model.silver.meteomatics.pressure import (
    meteomatics_pressure_table,
)
from data_platform.data_model.silver.meteomatics.production_forecast import (
    meteomatics_production_forecast_table,
)
from data_platform.data_model.silver.meteomatics.snow_depth_forecast import (
    meteomatics_snow_depth_forecast_table,
)
from data_platform.data_model.silver.meteomatics.soil_moisture_forecast import (
    meteomatics_soil_moisture_forecast_table,
)
from data_platform.data_model.silver.meteomatics.solar_radiation_forecast import (
    meteomatics_solar_radiation_forecast_table,
)
from data_platform.data_model.silver.meteomatics.temperature import (
    meteomatics_temperature_table,
)
from data_platform.data_model.silver.meteomatics.temperature_forecast import (
    meteomatics_temperature_forecast_table,
)
from data_platform.data_model.silver.meteomatics.wind_direction_forecast import (
    meteomatics_wind_direction_forecast_table,
)
from data_platform.data_model.silver.meteomatics.wind_speed import (
    meteomatics_wind_speed_table,
)
from data_platform.data_model.silver.meteomatics.wind_speed_forecast import (
    meteomatics_wind_speed_forecast_table,
)

# Forecast Intraday
from data_platform.data_model.silver.meteomatics.snow_depth_forecast_intraday import (
    meteomatics_snow_depth_forecast_intraday_table,
)
from data_platform.data_model.silver.meteomatics.wind_speed_forecast_intraday import (
    meteomatics_wind_speed_forecast_intraday_table,
)
from data_platform.data_model.silver.meteomatics.temperature_forecast_intraday import (
    meteomatics_temperature_forecast_intraday_table,
)
from data_platform.data_model.silver.meteomatics.solar_radiation_forecast_intraday import (
    meteomatics_solar_radiation_forecast_intraday_table,
)
from data_platform.data_model.silver.meteomatics.production_forecast_intraday import (
    meteomatics_production_forecast_intraday_table,
)

__all__ = [
    "attribute_table",
    "meteomatics_cloud_cover_forecast_table",
    "meteomatics_geopotential_height_table",
    "meteomatics_humidity_forecast_table",
    "meteomatics_precipitation_forecast_table",
    "meteomatics_precipitation_table",
    "meteomatics_pressure_table",
    "meteomatics_production_forecast_table",
    "meteomatics_snow_depth_forecast_table",
    "meteomatics_soil_moisture_forecast_table",
    "meteomatics_solar_radiation_forecast_table",
    "meteomatics_temperature_forecast_table",
    "meteomatics_temperature_table",
    "meteomatics_wind_direction_forecast_table",
    "meteomatics_wind_speed_forecast_table",
    "meteomatics_wind_speed_table",
    # forecast intraday tables
    "meteomatics_snow_depth_forecast_intraday_table",
    "meteomatics_wind_speed_forecast_intraday_table",
    "meteomatics_temperature_forecast_intraday_table",
    "meteomatics_solar_radiation_forecast_intraday_table",
    "meteomatics_production_forecast_intraday_table",
]
