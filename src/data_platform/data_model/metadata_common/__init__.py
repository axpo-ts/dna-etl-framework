"""Common metadata definitions for table models.

This package contains standardized definitions for common columns,
their comments, and other metadata patterns used across table models.
"""

from data_platform.data_model.metadata_common.common_columns import standard_columns
from data_platform.data_model.metadata_common.tags_enum import standard_tags

__all__ = ["standard_columns", "standard_tags"]
