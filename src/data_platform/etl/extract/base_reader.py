# reader_base.py  (private - imported by the two public modules)
from __future__ import annotations

from abc import ABC
from collections.abc import Mapping
from dataclasses import dataclass, field

from pyspark.sql import DataFrame

from data_platform.etl.core import TaskContext
from data_platform.etl.etl_task import ETLTask


@dataclass(kw_only=True)
class ReaderBase(ETLTask, ABC):
    """Abstract helper that returns a configured Spark reader (batch or streaming).

    With `schema` and `reader_options` applied.
    """

    context: TaskContext
    format_type: str = "delta"

    reader_options: Mapping[str, str] = field(default_factory=dict)
    schema: str | None = None

    # ------------------------------------------------------------------ #
    def _reader(self, *, streaming: bool = False) -> DataFrame:
        """Create and configure a Spark reader for batch or streaming data."""
        read = self.context.spark.readStream if streaming else self.context.spark.read
        rdr = read.schema(self.schema) if self.schema else read
        return rdr.options(**self.reader_options)

    # For type checkers - subclasses implement execute()
    def execute(self) -> DataFrame:
        """Execute the reader task and return a DataFrame."""
        ...
