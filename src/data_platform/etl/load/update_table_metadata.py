from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass, field

from pyspark.sql.functions import current_user
from pyspark.sql.utils import AnalysisException

from data_platform.data_model import AbstractTableModel
from data_platform.etl.core import TaskContext
from data_platform.etl.etl_task import ETLTask


# ────────────────────────────────────────────────────────────────────────────
# Custom error classes
# ────────────────────────────────────────────────────────────────────────────
class RemoveTagsError(Exception):
    """Raised when the UNSET TAGS statement fails."""


class SetTagError(Exception):
    """Raised when an individual SET TAGS operation fails."""


class UpdateTagsError(Exception):
    """Raised when REFRESH TABLE after tag update fails."""


class CommentTableError(Exception):
    """Raised when updating the table comment fails."""


class CommentColumnError(Exception):
    """Raised when setting a column comment fails."""


class NoColumnMatchError(Exception):
    """Raised when provided column comments do not match existing columns."""


# ────────────────────────────────────────────────────────────────────────────
# Helper
# ────────────────────────────────────────────────────────────────────────────
def _escape(text: str) -> str:
    """Escape single quotes so the string can be embedded in SQL safely."""
    return text.replace("'", "\\'")


# ────────────────────────────────────────────────────────────────────────────
# Main task
# ────────────────────────────────────────────────────────────────────────────
@dataclass(kw_only=True)
class UpdateTableMetadata(ETLTask):
    """Update Unity-Catalog metadata of a table.

    Parameters
    ----------
    context : TaskContext
        Spark-/logger-wrapper injected by the orchestration framework.
    table_model : AbstractTableModel
        Model holding identifier, comment, tags, column comments, …
    comment : str | None
        Override for the table comment.  If *None* the value in
        `table_model.comment` is used.
    tags : Mapping[str, str] | None
        Override for tags.  If *None* the mapping in
        `table_model.tags` is used.  When a non-empty mapping is
        provided all existing tags are removed first.
    column_comments : Mapping[str, str] | None
        Mapping column_name → comment.  When *None* no column comments
        are modified.
    """

    context: TaskContext
    table_model: AbstractTableModel

    comment: str | None = None
    tags: Mapping[str, str] | None = None
    column_comments: Mapping[str, str] | None = None
    task_name: str = field(init=False, default="UpdateTableMetadata")

    @property
    def _full_table_name(self) -> str:
        """Return the full table name."""
        return self._full_unitycatalog_name_of(self.table_model.identifier)

    @property
    def _full_catalog_name(self) -> str:
        """Return the full catalog name."""
        return _escape(self._get_full_catalog_name(self.table_model.identifier.catalog))

    @property
    def _full_schema_name(self) -> str:
        """Return the full schema name."""
        return _escape(self._get_full_schema_name(self.table_model.identifier.schema))

    @property
    def _table_name(self) -> str:
        return self.table_model.identifier.name

    def _get_current_tags(self) -> dict[str, str]:
        """Check if the tags have changed compared to the table model."""
        # If any problem occurs with the query, we return an empty dict
        try:
            # Get current tags from the table
            current_tags_dict = (
                self.context.spark.sql(f"""
                SELECT tag_name, tag_value
                FROM {self._full_catalog_name}.information_schema.table_tags
                WHERE catalog_name ='{self._full_catalog_name}'
                AND schema_name ='{self._full_schema_name}'
                AND table_name  ='{self._table_name}'
            """)
                .toPandas()
                .set_index("tag_name")
                .to_dict()["tag_value"]
            )
            return current_tags_dict
        except Exception as e:
            self.context.logger.error(
                f"Failed to get current tags for {self._full_table_name}: {e}. It does not impact the update process."
            )
            return {}

    def _get_current_table_comment(self) -> str:
        """Get the current table comment."""
        # If any problem occurs with the query, we return an empty string
        try:
            df_comment = self.context.spark.sql(f"""
            SELECT comment
            FROM system.information_schema.tables
            WHERE table_catalog='{self._full_catalog_name}'
                AND table_schema ='{self._full_schema_name}'
                AND table_name  ='{self._table_name}'
            """)
            if df_comment.isEmpty():
                # If the table is empty, it means it has no comment
                return ""
            # If the table is not empty, we expect it to have a comment info in the first row
            return df_comment.collect()[0]["comment"] or ""
        except Exception as e:
            self.context.logger.error(
                f"Failed to get current table comment: {e}.  It does not impact the update process."
            )
            return ""

    def _remove_all_tags(self) -> None:
        """UNSET every existing tag on the table."""
        spark = self.context.spark
        sql_stmt = f"""
            SELECT tag_name
            FROM {self._full_catalog_name}.information_schema.table_tags
            WHERE catalog_name ='{self._full_catalog_name}'
              AND schema_name ='{self._full_schema_name}'
              AND table_name  ='{self._table_name}'
        """
        tag_rows = spark.sql(sql_stmt).collect()

        if not tag_rows:
            self.context.logger.info(f"No tags to remove in {self._full_table_name}")
            return

        tag_list = ", ".join(f"'{_escape(r['tag_name'])}'" for r in tag_rows)
        sql = f"ALTER TABLE {self._full_table_name} UNSET TAGS ({tag_list})"
        self.context.logger.info(sql)
        try:
            spark.sql(sql)
        except Exception as exc:
            raise RemoveTagsError(sql) from exc

    def _set_one_tag(self, key: str, value: str) -> None:
        """Execute a single SET TAGS operation."""
        if "'" in key or "'" in value:
            raise SetTagError("quotes are not allowed inside tag key/value")

        sql = f"ALTER TABLE {self._full_table_name} SET TAGS ('{_escape(key)}' = '{_escape(value)}')"
        self.context.logger.info(sql)
        try:
            self.context.spark.sql(sql)
        except Exception as exc:
            raise SetTagError(sql) from exc

    def _update_tags(self) -> None:
        """Remove all existing tags and set the new mapping."""
        tag_map = self.tags if self.tags is not None else self.table_model.tags
        if not tag_map:
            self.context.logger.info("No tags provided, skipping tag update")
            return

        # We only update tags if they are different from the current ones
        # First we remove all tags, then we set one by one again
        current_tags = self._get_current_tags()
        if current_tags != tag_map:
            self._nothing_to_update = False
            self._remove_all_tags()
            for k, v in tag_map.items():
                self._set_one_tag(k, v)
            try:
                self.context.spark.sql(f"REFRESH TABLE {self._full_table_name}")
            except Exception as exc:
                raise UpdateTagsError("REFRESH TABLE failed") from exc

    def _set_table_comment(self) -> None:
        """Set or override the table comment."""
        new_comment = self.comment if self.comment is not None else self.table_model.comment
        if not new_comment:
            return
        current_comment = self._get_current_table_comment()
        # We only update the comment if the new one is different from the current one
        if new_comment != current_comment:
            self._nothing_to_update = False
            sql = f"ALTER TABLE {self._full_table_name} SET TBLPROPERTIES ('comment' = '{_escape(new_comment)}')"
            self.context.logger.info(sql)
            try:
                self.context.spark.sql(sql)
            except Exception as exc:
                raise CommentTableError(sql) from exc

    def _get_column_comments(self) -> dict | None:
        """Get current column comments for the table."""
        try:
            # Query description of the table to get column comments
            df_extended_info = self.context.spark.sql(f"DESCRIBE EXTENDED {self._full_table_name}").toPandas()
            # Return all column comments in a dictionary.
            return df_extended_info.drop_duplicates("col_name", keep="first").set_index("col_name")["comment"].to_dict()
        except Exception as e:
            self.context.logger.error(
                f"Failed to get current column comments for {self._full_table_name}: {e}."
                "It does not impact the update process."
            )
            return None

    def _update_column_comments(self) -> None:
        """Synchronise column comments with *column_comments* mapping."""
        audit_columns = set(self.table_model.audit_columns)
        column_comments = {
            field.name: field.metadata["comment"]
            for field in self.table_model.schema.fields
            if "comment" in field.metadata and field.name not in audit_columns
        }
        if not column_comments:
            return

        spark = self.context.spark
        existing = {r["col_name"] for r in spark.sql(f"SHOW COLUMNS IN {self._full_table_name}").collect()}
        existing = existing - audit_columns
        requested = set(column_comments.keys())

        if existing != requested:
            raise NoColumnMatchError(
                f"Mismatch columns in table {self._full_table_name}: existing={existing}, requested={requested} "
                "All columns needs to be commented."
            )
        # Get current column comments for the table
        dict_com_current = self._get_column_comments()
        # If the query failed, we assume all columns have empty comments
        if dict_com_current is None:
            dict_com_current = {k: "" for k in column_comments.keys()}
        # Updating comments
        for col, com in column_comments.items():
            # If the column is not present in the current column comments, we define it as empty
            if col not in dict_com_current.keys():
                com_current = ""
            else:
                com_current = dict_com_current[col]

            # Compare existing comment with the comment present in new metadata
            # If they are different, we update the column comment
            # The ( or ) clause prevents comparations between None and empty string
            if (com or "") != (com_current or ""):
                sql = f"ALTER TABLE {self._full_table_name} ALTER COLUMN {col} COMMENT '{_escape(com)}'"
                self.context.logger.info(sql)
                self._nothing_to_update = False
                try:
                    spark.sql(sql)
                except Exception as exc:
                    raise CommentColumnError(sql) from exc

    # ─────────────────────────────────────────────────────────────────
    # Task entry-point
    # ─────────────────────────────────────────────────────────────────
    def execute(self) -> None:
        """Run all requested metadata updates."""
        self.context.logger.info(f"Updating metadata for {self._full_table_name}")
        self._nothing_to_update = True
        try:
            self._update_tags()
            self._set_table_comment()
            self._update_column_comments()
            if self._nothing_to_update:
                self.context.logger.info(f"No metadata changes for {self._full_table_name}.")
        except AnalysisException as e:
            error_message = str(e)
            if "Insufficient privileges" in error_message:
                self.context.logger.warning(
                    f"The user {current_user()} does not have sufficient privileges "
                    f"to update metadata for {self._full_table_name} continuing without updating metadata."
                )
        except Exception:
            self.context.logger.exception(f"Metadata update failed for {self._full_table_name}")
            raise

        self.context.logger.info(f"Metadata update completed for {self._full_table_name}")
