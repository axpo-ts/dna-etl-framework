# ruff: noqa: N812
import pyspark.sql.types as T

from data_platform.data_model import FileVolumeIdentifier, StaticTableModel, TableIdentifier

intraday_helper = StaticTableModel(
    identifier=TableIdentifier("bronze", "meteomatics", "intraday_helper"),
    schema=T.StructType(
        [
            T.StructField("curve_name", T.StringType(), True),
            T.StructField("latest_model_load", T.TimestampType(), True),
            T.StructField("successful_load", T.BooleanType(), True),
        ]
    ),
    comment="Helper table to keep track of the latest model run for each curve",
    primary_keys=("curve_name", "successful_load"),
)

file_volume_identifiers = [
    FileVolumeIdentifier(
        catalog="staging",
        schema="meteomatics",
        name="intraday_staging/diffuse_rad_w_mm_euro1k_pt15m",
        file_path=None,
    ),
    FileVolumeIdentifier(
        catalog="staging",
        schema="meteomatics",
        name="intraday_staging/direct_rad_w_mm_euro1k_pt15m",
        file_path=None,
    ),
    FileVolumeIdentifier(
        catalog="staging",
        schema="meteomatics",
        name="intraday_staging/global_rad_w_mm_euro1k_pt15m",
        file_path=None,
    ),
    FileVolumeIdentifier(
        catalog="staging",
        schema="meteomatics",
        name="intraday_staging/snow_depth_m_mm_euro1k_pt15m",
        file_path=None,
    ),
    FileVolumeIdentifier(
        catalog="staging",
        schema="meteomatics",
        name="intraday_staging/solar_power_mw_mm_euro1k_pt15m",
        file_path=None,
    ),
    FileVolumeIdentifier(
        catalog="staging",
        schema="meteomatics",
        name="intraday_staging/t_2m_k_mm_euro1k_pt15m",
        file_path=None,
    ),
    FileVolumeIdentifier(
        catalog="staging",
        schema="meteomatics",
        name="intraday_staging/wind_speed_10m_ms_mm_euro1k_pt15m",
        file_path=None,
    ),
]

bronze_intraday = StaticTableModel(
    identifier=TableIdentifier("bronze", "meteomatics", "intraday"),
    schema=T.StructType(
        [
            T.StructField("curve_name", T.StringType(), True),
            T.StructField("time", T.LongType(), True),
            T.StructField("latitude", T.DoubleType(), True),
            T.StructField("longitude", T.DoubleType(), True),
            T.StructField("requested_time", T.StringType(), True),
            T.StructField("latest_model_load", T.StringType(), True),
            T.StructField("crs_wgs84", T.LongType(), True),
            T.StructField("value", T.DoubleType(), True),
            T.StructField("_rescued_data", T.StringType(), True),
            T.StructField("source_file", T.StringType(), True),
            T.StructField("source_system", T.StringType(), True),
        ]
    ),
    comment="Bronze table to store all curves from the intraday API endpoint of Meteomatics",
    sources=file_volume_identifiers,
    primary_keys=("time", "latitude", "longitude"),
)
