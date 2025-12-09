# ruff: noqa: N812
import pyspark.sql.types as T

from data_platform.data_model import FileVolumeIdentifier, StaticTableModel, TableIdentifier

file_volume_identifiers = [
    FileVolumeIdentifier(
        catalog="staging",
        schema="meteomatics",
        name="actual_staging/geopotential_height_500hpa_m_ecmwf_era5_p1d",
        file_path=None,
    ),
    FileVolumeIdentifier(
        catalog="staging",
        schema="meteomatics",
        name="actual_staging/msl_pressure_hpa_ecmwf_era5_p1d",
        file_path=None,
    ),
    FileVolumeIdentifier(
        catalog="staging",
        schema="meteomatics",
        name="actual_staging/precip_24h_mm_ecmwf_era5_p1d",
        file_path=None,
    ),
    FileVolumeIdentifier(
        catalog="staging",
        schema="meteomatics",
        name="actual_staging/t_mean_2m_24h_c_ecmwf_era5_p1d",
        file_path=None,
    ),
    FileVolumeIdentifier(
        catalog="staging",
        schema="meteomatics",
        name="actual_staging/t_mean_850hpa_24h_c_ecmwf_era5_p1d",
        file_path=None,
    ),
    FileVolumeIdentifier(
        catalog="staging",
        schema="meteomatics",
        name="actual_staging/wind_speed_10hpa_ms_ecmwf_era5_p1d",
        file_path=None,
    ),
    FileVolumeIdentifier(
        catalog="staging",
        schema="meteomatics",
        name="actual_staging/wind_speed_200hpa_ms_ecmwf_era5_p1d",
        file_path=None,
    ),
    FileVolumeIdentifier(
        catalog="staging",
        schema="meteomatics",
        name="actual_staging/wind_speed_u_10hpa_ms_ecmwf_era5_p1d",
        file_path=None,
    ),
    FileVolumeIdentifier(
        catalog="staging",
        schema="meteomatics",
        name="actual_staging/wind_speed_u_200hpa_ms_ecmwf_era5_p1d",
        file_path=None,
    ),
    FileVolumeIdentifier(
        catalog="staging",
        schema="meteomatics",
        name="actual_staging/wind_speed_v_10hpa_ms_ecmwf_era5_p1d",
        file_path=None,
    ),
    FileVolumeIdentifier(
        catalog="staging",
        schema="meteomatics",
        name="actual_staging/wind_speed_v_200hpa_ms_ecmwf_era5_p1d",
        file_path=None,
    ),
]


bronze_actuals = StaticTableModel(
    identifier=TableIdentifier("bronze", "meteomatics", "actuals"),
    schema=T.StructType(
        [
            T.StructField("curve_name", T.StringType(), True),
            T.StructField("time", T.LongType(), True),
            T.StructField("latitude", T.DoubleType(), True),
            T.StructField("longitude", T.DoubleType(), True),
            T.StructField("crs_wgs84", T.LongType(), True),
            T.StructField("value", T.DoubleType(), True),
            T.StructField("_rescued_data", T.StringType(), True),
            T.StructField("source_file", T.StringType(), True),
            T.StructField("source_system", T.StringType(), True),
        ]
    ),
    comment="Bronze table to store all curves from the actuals API endpoint of Meteomatics",
    sources=file_volume_identifiers,
    primary_keys=("time", "latitude", "longitude"),
)
