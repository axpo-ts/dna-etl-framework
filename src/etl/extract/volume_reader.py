# reader_base.py  (private - imported by the two public modules)
from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from pathlib import Path

from pyspark.sql import DataFrame

from etl.core import TaskContext
from etl.etl_task import ETLTask


@dataclass(kw_only=True)
class VolumeFilesReader(ETLTask):
    """Class to read files from volumes to a spark dataframe.

    This task reads files from Unity Catalog volumes, optionally filtering
    to the most recent N files based on filename ordering or using watermark
    filtering based on file modification time. Can recursively traverse subdirectories.

    Args:
        context: Task execution context with Spark session and logger.
        volumes: List of volume paths to read from.
        format_type: File format to read (json, csv, parquet, etc.).
        reader_options: Additional options for the Spark reader.
        max_files: Maximum number of most recent files to read. If None, reads all files.
        watermark_days: Number of days to look back for watermark filtering. If None, no watermark applied.
        recursive: If True, recursively searches all subdirectories for files.
    """

    context: TaskContext
    volumes: list[str]
    format_type: str = "json"
    reader_options: Mapping[str, str] = field(default_factory=dict)
    max_files: int | None = None
    watermark_days: int | None = None
    recursive: bool = False

    task_name: str = field(init=False, default="VolumeReader")

    def _list_files_recursive(self, path: str) -> list[tuple[str, dict]]:
        """Recursively list all files in a directory and its subdirectories.

        Args:
            path: Root path to start listing from.

        Returns:
            List of tuples containing (file_path, file_info_dict).
            file_info_dict contains 'path', 'name', 'size', 'modificationTime'.

        Raises:
            RuntimeError: If unable to list files from the path.
        """
        all_files: list[tuple[str, dict]] = []

        try:
            self.context.logger.debug(f"Listing contents of: {path}")
            files_info = self.context.dbutils.fs.ls(path)

            for file_info in files_info:
                if file_info.path.endswith("/"):
                    # It's a directory - recurse into it
                    subdirectory_files = self._list_files_recursive(file_info.path)
                    all_files.extend(subdirectory_files)
                else:
                    # It's a file - add to list
                    file_dict = {
                        "path": file_info.path,
                        "name": Path(file_info.path).name,
                        "size": file_info.size,
                        "modificationTime": file_info.modificationTime,
                    }
                    all_files.append((file_info.path, file_dict))

        except Exception as e:
            self.context.logger.warning(f"Failed to list files in path {path}: {e!s}")

        return all_files

    def _get_all_files(self) -> list[tuple[str, dict]]:
        """Get all files from volumes, optionally including subdirectories.

        Returns:
            List of tuples containing (file_path, file_info_dict).

        Raises:
            RuntimeError: If unable to list files from volumes.
        """
        all_files: list[tuple[str, dict]] = []

        for volume_path in self.volumes:
            try:
                self.context.logger.info(
                    f"Listing files in volume {'(recursive)' if self.recursive else ''}: {volume_path}"
                )

                if self.recursive:
                    # Use recursive listing
                    volume_files = self._list_files_recursive(volume_path)
                else:
                    # Use non-recursive listing (original behavior)
                    files_info = self.context.dbutils.fs.ls(volume_path)
                    volume_files = []

                    for file_info in files_info:
                        if not file_info.path.endswith("/"):
                            file_dict = {
                                "path": file_info.path,
                                "name": Path(file_info.path).name,
                                "size": file_info.size,
                                "modificationTime": file_info.modificationTime,
                            }
                            volume_files.append((file_info.path, file_dict))

                # Filter by file format
                valid_files = [
                    (file_path, file_info)
                    for file_path, file_info in volume_files
                    if self._is_valid_file_format(file_info["name"])
                ]

                all_files.extend(valid_files)
                self.context.logger.info(f"Found {len(valid_files)} valid files in volume {volume_path}")

            except Exception as e:
                self.context.logger.warning(f"Failed to list files in volume {volume_path}: {e!s}")
                continue

        return all_files

    def _get_files_by_watermark(self) -> list[str]:
        """Get files within the watermark period based on modification time.

        Filters files based on their modification time being within the
        specified watermark_days from the current time.

        Returns:
            List of file paths within the watermark period.

        Raises:
            RuntimeError: If unable to list files from volumes.
        """
        if self.watermark_days is None:
            raise ValueError("watermark_days must be specified for watermark filtering")

        try:
            cutoff_time = datetime.now() - timedelta(days=self.watermark_days)
            cutoff_timestamp = int(cutoff_time.timestamp() * 1000)  # Convert to milliseconds
        except OSError:
            # Fallback: Use Unix epoch start (1970-01-01)
            cutoff_timestamp = 0
            self.context.logger.warning("Cutoff date before Unix epoch, using timestamp 0")

        all_files = self._get_all_files()
        valid_files: list[str] = []

        for file_path, file_info in all_files:
            if file_info["modificationTime"] >= cutoff_timestamp:
                valid_files.append(file_path)
                self.context.logger.info(
                    f"File {file_info['name']} within watermark period: "
                    f"modified {datetime.fromtimestamp(file_info['modificationTime'] / 1000)}"
                )

        self.context.logger.info(f"Found {len(valid_files)} files within {self.watermark_days} day watermark period")

        return valid_files

    def _get_most_recent_files(self) -> list[str]:
        """Get the most recent N files from all volumes ordered by filename.

        This method lists all files from the specified volumes, sorts them
        by filename in descending order (assuming filename contains timestamp
        or sequential information), and returns the most recent N files.

        Returns:
            List of file paths for the most recent files.

        Raises:
            RuntimeError: If unable to list files from volumes.
        """
        all_files = self._get_all_files()

        if not all_files:
            self.context.logger.warning("No valid files found in any volume")
            return []

        # Convert to list of (filename, full_path) for sorting
        files_for_sorting = [(file_info["name"], file_path) for file_path, file_info in all_files]

        # Sort by filename in descending order (most recent first)
        sorted_files = sorted(files_for_sorting, key=lambda x: x[0], reverse=True)

        # Select most recent N files if max_files is specified
        if self.max_files is not None:
            sorted_files = sorted_files[: self.max_files]
            self.context.logger.info(f"Selected {len(sorted_files)} most recent files out of {len(all_files)} total")
        else:
            self.context.logger.info(f"Selected all {len(sorted_files)} files")

        return [file_path for _, file_path in sorted_files]

    def _is_valid_file_format(self, filename: str) -> bool:
        """Check if file matches the expected format.

        Args:
            filename: Name of the file to check.

        Returns:
            True if file format matches, False otherwise.
        """
        # Map format types to common extensions
        format_extensions = {
            "json": [".json", ".jsonl"],
            "csv": [".csv"],
            "parquet": [".parquet"],
            "delta": [".delta"],
            "text": [".txt"],
            "xml": [".xml"],
        }

        expected_extensions = format_extensions.get(self.format_type.lower(), [])

        # If no specific extensions defined, accept all files
        if not expected_extensions:
            return True

        return any(filename.lower().endswith(ext) for ext in expected_extensions)

    def execute(self) -> DataFrame:
        """Read files from volumes with watermark filtering.

        This method applies watermark filtering based on file modification time
        if watermark_days is specified, otherwise falls back to max_files filtering.

        Returns:
            Combined DataFrame from all selected files.

        Raises:
            RuntimeError: If no files found or reading fails.
        """
        try:
            if self.watermark_days is not None:
                self.context.logger.warning("Start watermark filtering")
                file_paths = self._get_files_by_watermark()
            else:
                self.context.logger.warning("Start max_files filtering")
                file_paths = self._get_most_recent_files()

            if not file_paths:
                self.context.logger.warning("No files found in specified volumes, returning empty DataFrame")
                from pyspark.sql.types import StructType

                empty_schema = StructType()  # Define an empty schema
                return self.context.spark.createDataFrame([], schema=empty_schema)

            self.context.logger.info(f"Reading {len(file_paths)} files from volumes with format {self.format_type}")

            # Read and combine all selected files
            df = self.context.spark.read.format(self.format_type).options(**self.reader_options).load(file_paths)
            return df
        except Exception as e:
            self.context.logger.error(f"Failed to read from volumes: {e!s}")
            raise RuntimeError("Volume reading failed") from e
