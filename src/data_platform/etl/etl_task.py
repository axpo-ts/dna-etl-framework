# etl_task.py
from __future__ import annotations

from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import Any

from data_platform.data_model import FileVolumeIdentifier
from data_platform.data_model.unity_catalog_identifier import UnityCatalogIdentifier
from data_platform.etl.core.task_context import TaskContext


@dataclass
class ETLTask(ABC):
    """Abstract base class for ETL tasks.

    Defines the structure for ETL tasks, including the `execute` method
    that must be implemented by subclasses.
    """

    context: TaskContext

    @abstractmethod
    def execute(self) -> Any:
        """Execute the ETL task within the provided context and configuration.

        Args:
            context (TaskContext): The task context in which the task should be executed.
            conf (Configuration): The configuration for the ETL task.

        """
        pass

    def _full_unitycatalog_name_of(self, uc_identifier: UnityCatalogIdentifier) -> str:
        return str(self.context.full_unitycatalog_name(uc_identifier))

    def _full_base_source_filename_of(self, file_volume_identifier: FileVolumeIdentifier) -> str:
        """Return the full file name for a given table model."""
        return str(self.context.full_file_name(file_volume_identifier))

    def _get_full_catalog_name(self, name: str) -> str:
        """Return the full catalog name."""
        return f"{self.context.catalog_prefix}{name}"

    def _get_full_schema_name(self, name: str) -> str:
        """Return the full schema name."""
        return f"{self.context.schema_prefix}{name}"
