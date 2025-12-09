"""Silver data model definitions."""
from __future__ import annotations

from data_platform.data_model.silver.neuron.agregaciones import (
    agregaciones_table,
)
from data_platform.data_model.silver.neuron.best_measure import (
    best_measure_table,
)
from data_platform.data_model.silver.neuron.contract import (
    contract_table,
)
from data_platform.data_model.silver.neuron.measure import (
    measure_table,
)
from data_platform.data_model.silver.neuron.product_contract import (
    product_contract_table,
)
from data_platform.data_model.silver.neuron.salesforce_rep_asset import (
    salesforce_rep_asset_table,
)
from data_platform.data_model.silver.neuron.salesforce_rep_contract import (
    salesforce_rep_contract_table,
)

__all__ = [
    "agregaciones_table",
    "best_measure_table",
    "contract_table",
    "measure_table",
    "product_contract_table",
    "salesforce_rep_asset_table",
    "salesforce_rep_contract_table",
]
