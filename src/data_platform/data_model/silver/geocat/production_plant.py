from __future__ import annotations

from pyspark.sql.types import DoubleType, StringType, StructField, StructType, TimestampType

from data_platform.data_model import StaticTableModel, TableIdentifier
from data_platform.data_model.metadata_common import standard_columns, standard_tags

production_plant_table = StaticTableModel(
    identifier=TableIdentifier(catalog="silver", schema="geocat", name="production_plant"),
    schema=StructType([
        StructField("xtf_id", StringType(), False, {"comment": "A unique identifier for each record in the original Interlis XTF file"}),
        StructField("address", StringType(), True, {"comment": "Location address (street and house number or cadastral designation)"}),
        StructField("post_code", StringType(), True, {"comment": "Four-digit postal code of the municipality where the plant is located"}),
        StructField("_x", DoubleType(), True, {"comment": "The north coordinate of the location under Swiss LV95 coordinate system"}),
        StructField("_y", DoubleType(), True, {"comment": "The east coordinate of the location under Swiss LV95 coordinate system"}),
        StructField("municipality", StringType(), True, {"comment": "Municipality where the plant is located"}),
        StructField("canton", StringType(), True, {"comment": "Abbreviation of the canton where the plant is located"}),
        StructField("main_category", StringType(), True, {"comment": "Main category according to the catalog"}),
        StructField("sub_category", StringType(), True, {"comment": "Subcategory according to the catalog"}),
        StructField("plant_category", StringType(), True, {"comment": "Plant category according to the catalog"}),
        StructField("beginning_of_operation", TimestampType(), True, {"comment": "Commissioning date of the electricity production plant"}),
        StructField("initial_power", DoubleType(), True, {"comment": "Initial commissioning capacity in kW (rounded to two decimal places). If extensions are not recorded separately, the initial commissioning capacity corresponds to the total capacity"}),
        StructField("total_power", DoubleType(), True, {"comment": "Total capacity (including possible extensions) in kW (rounded to two decimal places)"}),
        standard_columns.LicenseColumn.to_struct_field(),
        standard_columns.DataSourceColumn.to_struct_field(),
        standard_columns.DataSystemColumn.to_struct_field(),
        standard_columns.ValidFromColumn.to_struct_field(),
        standard_columns.ValidToColumn.to_struct_field(),
        standard_columns.IsCurrentColumn.to_struct_field(),
    ]),
    comment="All electricity production plants that are registered in the Swiss system and are in operation.",
    sources=[
        TableIdentifier(catalog="bronze", schema="pronovo", name="production_plant_pronovo"),
        TableIdentifier(catalog="silver", schema="pronovo", name="attribute"),
    ],
    primary_keys=["xtf_id"],
    tags={
        standard_tags.ConfidentialityLevel.KEY: standard_tags.ConfidentialityLevel.C2_INTERNAL,
        standard_tags.DataDomain.KEY: standard_tags.DataDomain.FUNDAMENTAL,
        standard_tags.DataOwner.KEY: standard_tags.DataOwner.MICHELE_RIZZI,
        standard_tags.DataSubdomain.KEY: standard_tags.DataSubdomain.PRODUCTION,
        standard_tags.LicensedData.KEY: standard_tags.LicensedData.TRUE,
        standard_tags.PIIClassification.KEY: standard_tags.PIIClassification.NO_PII,
    },
)
