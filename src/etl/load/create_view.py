from dataclasses import dataclass, field

from data_model.static_view_model import StaticViewModel
from data_model.table_identifier import TableIdentifier
from etl.core.task_context import TaskContext
from etl.etl_task import ETLTask


# ────────────────────────────────────────────────────────────────────────────
#                         Single public task
# ────────────────────────────────────────────────────────────────────────────
@dataclass(kw_only=True)
class CreateView(ETLTask):
    """An ETL Task to create a view from a source table.

    Parameters
    ----------
    context : TaskContext
        Spark / logger wrapper supplied by your pipeline framework.
    source_table  : TableIdentifier
        Table ID to source from.
    target_view  : AbstractTableReference
        The canonical table reference that defines the view.
    default_license : str
        The license to assign if none is given in the target_view specification.
    """

    context: TaskContext
    target_view: StaticViewModel
    task_name: str = field(init=False, default="CreateView")

    @property
    def source_table(self) -> TableIdentifier:
        """Return the source table identifier backing this view.

        Returns:
            TableIdentifier: Identifier of the underlying source table defined in the target view model.
        """
        return self.target_view.source_table

    @property
    def _source_table_name(self) -> str:
        return self._full_unitycatalog_name_of(self.source_table)

    @property
    def _target_view_name(self) -> str:
        return self._full_unitycatalog_name_of(self.target_view.identifier)

    def _get_primary_key_columns(self) -> list[str]:
        """Fetch the primary key columns of the source table."""
        catalog = self.source_table.catalog
        schema = self.source_table.schema
        table = self.source_table.name
        spark = self.context.spark

        # Find the primary key constraint name(s)
        pk_constraints_query = f"""
            SELECT constraint_name
            FROM {catalog}.information_schema.table_constraints
            WHERE table_schema = '{schema}'
              AND table_name = '{table}'
              AND constraint_type = 'PRIMARY KEY'
        """
        pk_constraints_df = spark.sql(pk_constraints_query)
        pk_constraint_names = [r["constraint_name"] for r in pk_constraints_df.collect()]

        if not pk_constraint_names:
            return []

        # Fetch columns participating in the PK
        constraint_names_in = ", ".join([f"'{c}'" for c in pk_constraint_names])
        pk_columns_query = f"""
            SELECT column_name
            FROM {catalog}.information_schema.key_column_usage
            WHERE table_schema = '{schema}'
              AND table_name = '{table}'
              AND constraint_name IN ({constraint_names_in})
            ORDER BY ordinal_position
        """
        pk_cols_df = spark.sql(pk_columns_query)
        return [r["column_name"] for r in pk_cols_df.collect()]

    def _get_table_comment(self) -> str:
        catalog = self.source_table.catalog
        schema = self.source_table.schema
        spark = self.context.spark
        query = f"""
            SELECT comment
            FROM {catalog}.information_schema.tables
            WHERE table_schema = '{schema}'
            AND table_name = '{self.source_table.name}'
        """

        df = spark.sql(query)
        row = df.first()
        comment = row["comment"] if row is not None else None
        return str(comment)

    def _get_table_tags(self) -> dict[str, str]:
        catalog = self.source_table.catalog
        schema = self.source_table.schema
        spark = self.context.spark
        query = f"""
            SELECT tag_name, tag_value
            FROM {self.source_table.catalog}.information_schema.table_tags
            WHERE catalog_name = '{catalog}'
            AND schema_name = '{schema}'
            AND table_name = '{self.source_table.name}'
        """
        tags_df = spark.sql(query)
        tags = {row.tag_name: row.tag_value for row in tags_df.collect()}
        return tags

    def _comment_clause(self) -> str:
        source_comment = self._get_table_comment()
        if source_comment is None:
            source_comment = self.target_view.comment

        source_comment = source_comment.replace("'", "")
        return f"COMMENT '{source_comment}' " if source_comment else ""

    def _license_clause(self) -> str:
        return f"'{self.target_view.license}' AS license"

    def _set_primary_key_tags_on_view(self, pk_columns: list[str]) -> None:
        """Set 'primary_key' = 'true' tags on the specified columns of the target view."""
        if not pk_columns:
            return
        spark = self.context.spark

        for col in pk_columns:
            stmt = f"ALTER TABLE {self._target_view_name} ALTER COLUMN `{col}` SET TAGS ('primary_key' = 'true');"
            self.context.logger.info(stmt)
            spark.sql(stmt)

    # ------------------------------------------------------------------ #
    # Public entry-point
    # ------------------------------------------------------------------ #
    def execute(self) -> None:
        """Execute the CreateTable task to create a view in the catalog."""
        spark = self.context.spark

        create_stmt = (
            f"CREATE OR REPLACE VIEW {self._target_view_name} "
            f"{self._comment_clause()} "
            "AS "
            "SELECT "
            "*, "
            f"{self._license_clause()} "
            "FROM "
            f"{self._source_table_name}"
        )

        self.context.logger.info(create_stmt)
        spark.sql(create_stmt)

        license_stmt = (
            f"COMMENT ON COLUMN {self._target_view_name}.license IS "
            "'The license package identifier (LPI) associated with the data'"
        )

        self.context.logger.info(license_stmt)
        spark.sql(license_stmt)

        try:
            pk_columns = self._get_primary_key_columns()
            self._set_primary_key_tags_on_view(pk_columns)
        except Exception as e:
            self.context.logger.warning(f"Could not set primary_key column tags on view {self._target_view_name}: {e}")

        source_tags = self._get_table_tags()

        if len(source_tags) == 0:
            source_tags = self.target_view.tags

        if source_tags is not None:
            props = ",  ".join([f"'{k}' = '{v}'" for k, v in source_tags.items()])
            alter_stmt = f"ALTER VIEW {self._target_view_name} SET TAGS ({props});"
            self.context.logger.info(alter_stmt)
            spark.sql(alter_stmt)
        if self.target_view.license:
            provide_access_stmt = f"GRANT SELECT ON VIEW {self._target_view_name} TO `{self.target_view.license}`;"
            self.context.logger.info(provide_access_stmt)
            spark.sql(provide_access_stmt)
