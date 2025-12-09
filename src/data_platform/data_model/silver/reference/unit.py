from pyspark.sql.types import StringType, StructField, StructType

from data_platform.data_model import StaticTableModel, TableIdentifier
from data_platform.data_model.metadata_common import standard_tags
from data_platform.data_model.silver.reference.reference_common import REFERENCE_COMMON_TAGS

unit_table = StaticTableModel(
    identifier=TableIdentifier(
        catalog="silver",
        schema="reference",
        name="unit"
    ),
    schema=StructType([
        StructField("pk_unit", StringType(), False, {"comment": "A unique identifier for each unit record"}),
        StructField("type", StringType(), True, {"comment": "The category of the unit e.g. Energy, Length"}),
        StructField("long_name", StringType(), True, {"comment": "The full descriptive name of the unit e.g. Megawatt-hour"}),
        StructField("short_name", StringType(), True, {"comment": "The abbreviated representation of the unit e.g. MWh"}),
        StructField("source_created_at", StringType(), True, {"comment": "The timestamp when the record was originally created in the source system EBX"}),
        StructField("source_created_by", StringType(), True, {"comment": "The user who originally created the record in the source system EBX"}),
        StructField("source_updated_at", StringType(), True, {"comment": "The timestamp when the record was last updated in the source system EBX"}),
        StructField("source_updated_by", StringType(), True, {"comment": "The user who last updated the record in the source system EBX"}),
        StructField("last_published_cet", StringType(), True, {"comment": "The timestamp when the record was last published in the source system EBX"}),
        StructField("license", StringType(), True, {"comment": "The license package identifier (LPI) associated with the data"})
    ]),
    comment=(
        "Classification of the most commonly used units of measurement at Axpo. Most are from the "
        "International System, and some are technical or commercial units, not recognised by the SI, "
        "although widely used in specific sectors. The list also includes exchange rate units used in "
        "Axpo, written according to the standard Forex market direction. A unit of measure is a defined "
        "magnitude of a quantity. Its used as a standard for measurement of the same kind of quantity. "
        "It means a unit of measure is a standard quantity used to express the size, amount, or degree "
        "of something. For example, meters are a unit of measure for length, and kilograms are a unit "
        "of measure for mass."
    ),
    sources=[
        TableIdentifier(catalog="bronze", schema="ebx", name="unit")
    ],
    primary_keys=("pk_unit",),
    tags={
        **REFERENCE_COMMON_TAGS,
        standard_tags.DataOwner.KEY: standard_tags.DataOwner.MARIA_LOURDES_SANCHEZ_NUNEZ,
    }
)
