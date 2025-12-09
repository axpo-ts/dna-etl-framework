# file_readers.py

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass, field

import pyspark.sql.functions as functions
from pyspark.sql import DataFrame

from data_platform.data_model import FileVolumeIdentifier
from data_platform.etl.extract.base_reader import ReaderBase


@dataclass(kw_only=True)
class StreamingFileReader(ReaderBase):
    """File-source Structured-Streaming reader."""

    file: str | FileVolumeIdentifier = None
    include_metadata: bool = False
    reader_options: Mapping[str, str] = field(default_factory=dict)
    schema_location: str = None
    task_name: str = field(init=False, default="StreamingFileReader")

    def execute(self) -> DataFrame:
        """Execute the streaming file reader and return a DataFrame."""
        if isinstance(self.file, FileVolumeIdentifier):
            base_filename = self._full_base_source_filename_of(self.file)
        else:
            base_filename = self.file

        if self.schema_location:
            self.reader_options["cloudFiles.schemaLocation"] = f"{base_filename}/{self.schema_location}"
            self.context.logger.info(f"Schema location set to {self.reader_options['cloudFiles.schemaLocation']}")
        self.context.logger.info(f"Reading streaming files from {base_filename}")

        df = (
            self._reader(streaming=True)
            .format(self.format_type)
            .options(**self.reader_options)
            .load(path=base_filename)
        )

        if self.include_metadata:
            df = df.withColumn("_source_filepath", functions.input_file_name())

        return df
