from pyspark.sql.types import StringType, StructField, StructType

from data_platform.data_model import StaticTableModel, TableIdentifier
from data_platform.data_model.metadata_common import standard_tags
from data_platform.data_model.silver.reference.reference_common import REFERENCE_COMMON_TAGS

commodity_table = StaticTableModel(
    identifier=TableIdentifier(
        catalog="silver",
        schema="reference",
        name="commodity"
    ),
    schema=StructType([
        StructField("pk_commodity", StringType(), False, {"comment": "A unique identifier for each commodity record"}),
        StructField("level0", StringType(), True, {"comment": "Top-level commodity category e.g. POWER, GAS"}),
        StructField("level1", StringType(), True, {"comment": "Second-level commodity subcategory within the level0 category e.g. LNG, PROPANE"}),
        StructField("level2", StringType(), True, {"comment": "Third-level commodity subcategory within the level1 category e.g. LNG, PROPANE"}),
        StructField("display_name", StringType(), True, {"comment": "The name for the commodity"}),
        StructField("source_created_at", StringType(), True, {"comment": "The timestamp when the record was originally created in the source system EBX"}),
        StructField("source_created_by", StringType(), True, {"comment": "The user who originally created the record in the source system EBX"}),
        StructField("source_updated_at", StringType(), True, {"comment": "The timestamp when the record was last updated in the source system EBX"}),
        StructField("source_updated_by", StringType(), True, {"comment": "The user who last updated the record in the source system EBX"}),
        StructField("last_published_cet", StringType(), True, {"comment": "The timestamp when the record was last published in the source system EBX"}),
        StructField("license", StringType(), True, {"comment": "The license package identifier (LPI) associated with the data"})
    ]),
    comment=(
        "Classification of commodities according to large groups that are differentiated from each other. "
        "Commodities are basic goods used in trade, such as agricultural raw materials (grains, coffee), "
        "energy (oil, natural gas) and metals (gold, silver). They are characterized by being homogenous "
        "and standardized products, which facilitates large-scale trading. They are standardized means "
        "that one unit of the commodity is essentially the same as another, regardless of who produced it. "
        "This interchangeability is what allows them to be traded. However, this list of commodities also "
        "includes economic variables and indicators, which are not directly tradable but represent "
        "underlying risks that affect commodity prices, and can be hedged using derivatives or financial "
        "contracts, such as inflation swaps, interest rate futures, or weather derivatives."
    ),
    sources=[
        TableIdentifier(catalog="bronze", schema="ebx", name="commodity")
    ],
    primary_keys=("pk_commodity",),
    tags={
        **REFERENCE_COMMON_TAGS,
        standard_tags.DataOwner.KEY: standard_tags.DataOwner.JAVIER_HERNANDEZ_MONTES,
    }
)
