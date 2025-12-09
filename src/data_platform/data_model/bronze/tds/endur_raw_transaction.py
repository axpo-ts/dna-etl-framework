from data_platform.data_model.static_view_model import StaticViewModel
from data_platform.data_model.view_identifier import ViewIdentifier

endur_raw_transaction_bronze_view = StaticViewModel(
    identifier=ViewIdentifier(catalog="bronze", schema="tds", name="endur_raw_transaction"),
    license="DNA-ALL-ACCESS",
    comment="TDS Bronze Endur Raw Transaction",
)
