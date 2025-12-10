from __future__ import annotations

from dataclasses import dataclass, field

import pyspark.sql.functions as f
from pyspark.sql import DataFrame, Row
from pyspark.sql.utils import AnalysisException

from data_model.table_identifier import TableIdentifier
from etl.etl_task import ETLTask


@dataclass(kw_only=True)
class ApplyReferenceMappingTask(ETLTask):
    """Apply reference mappings to replace or enrich column values with canonical ones.

    This task supports two modes configured in the YAML mapping:
    1. Replace mode: Replaces values in source_column with mapped values
    2. Enrich mode: Keeps source_column unchanged and adds a new column with mapped values

    The mode is determined per source column from the YAML configuration.

    Args:
        mapping_ref_group: The mapping group to apply.
        rename_to_group: If True, renames the source column to mapping_ref_group name (replace mode only).
        mapping_table_name: Table identifier for the mapping table.
    """

    mapping_ref_group: str
    rename_to_group: bool = False

    mapping_table_name: TableIdentifier = field(
        default_factory=lambda: TableIdentifier("silver", "reference", "reference_data_mapping")
    )

    task_name: str = field(init=False, default="ApplyReferenceMappingTask")

    def _can_apply_mapping(self, mapping: Row, input_df: DataFrame) -> bool:
        """Check if a mapping can be applied to the DataFrame.

        Args:
            mapping: Mapping row from reference_data_mapping table.
            input_df: Input DataFrame to check.

        Returns:
            True if the mapping can be applied, False otherwise.
        """
        df_columns = set(input_df.columns)

        # For simple value mappings (source_value is not None)
        if mapping["source_value"] is not None:
            # Must have source_column and it must exist in DataFrame
            return mapping["source_column"] is not None and mapping["source_column"] in df_columns

        # For match_expression mappings (source_value is None)
        if mapping["match_expression"] is not None:
            # Check if source_column exists when specified (for replacement)
            if mapping["source_column"] is not None and mapping["source_column"] not in df_columns:
                return False

            try:
                # Try to evaluate the expression directly with PySpark
                # This tests if the expression is valid for this DataFrame's columns
                expr_col = f.expr(mapping["match_expression"])
                input_df.filter(expr_col).limit(0).collect()
                return True

            except AnalysisException:
                return False
            except Exception:
                # Other errors - assume not applicable
                return False

        return False

    def execute(self, input_df: DataFrame) -> DataFrame:
        """Apply reference mappings to input DataFrame.

        Based on the mode configured in YAML:
        - Replace mode: Replaces values in source_column with mapped reference values.
        - Enrich mode: Adds a new column with mapped values, keeping source column unchanged.

        Args:
            input_df: Input DataFrame to apply mappings to.

        Returns:
            DataFrame with mapped canonical values.

        Examples:
            >>> # Replace mode (configured in YAML with mode: "replace")
            >>> task = ApplyReferenceMappingTask(context=context, mapping_ref_group="unit")
            >>> # Input:  unit = ["€/MWh", "EUR/MWh", "$/kWh"]
            >>> # Output: unit = ["EUR/MWh", "EUR/MWh", "$/kWh"]

            >>> # Enrich mode (configured in YAML with mode: "enrich")
            >>> task = ApplyReferenceMappingTask(context=context, mapping_ref_group="countryext")
            >>> # Input:  region = ["It-Nord", "DE-South", "USA-West"]
            >>> # Output: region = ["It-Nord", "DE-South", "USA-West"], countryext = ["IT", "DE", None]
        """
        ctx = self.context
        ctx.logger.info(f"🔧 Applying mappings for group: {self.mapping_ref_group}")

        # Load mapping configuration
        mapping_fqn = ctx.full_unitycatalog_name(self.mapping_table_name)

        # Check if mapping table exists
        if not ctx.spark.catalog.tableExists(mapping_fqn):
            ctx.logger.warning(
                f"Mapping table '{mapping_fqn}' does not exist. "
                f"Skipping reference mapping for group '{self.mapping_ref_group}'"
            )
            return input_df

        # Load mappings for this group
        mapping_df = (
            ctx.spark.table(mapping_fqn)
            .filter((f.col("mapping_ref_group") == self.mapping_ref_group) & (f.col("valid_to").isNull()))
            .cache()
        )

        all_mappings = mapping_df.collect()
        if not all_mappings:
            ctx.logger.warning(f"No active mappings found for group: {self.mapping_ref_group}")
            return input_df

        # Filter to only applicable mappings
        applicable_mappings = [m for m in all_mappings if self._can_apply_mapping(m, input_df)]

        if not applicable_mappings:
            ctx.logger.info(
                f"No applicable mappings for group '{self.mapping_ref_group}' "
                f"with current DataFrame columns: {input_df.columns}"
            )
            return input_df

        # Group mappings by mode and source_column
        replace_mappings = {}  # source_column -> list of mappings
        enrich_mappings = []  # list of all enrich mappings

        for mapping in applicable_mappings:
            # Get mode from mapping, default to 'replace' for backward compatibility
            mode = mapping["mode"]
            source_col = mapping["source_column"]

            if mode == "enrich":
                enrich_mappings.append(mapping)
            else:  # replace mode
                if source_col:
                    if source_col not in replace_mappings:
                        replace_mappings[source_col] = []
                    replace_mappings[source_col].append(mapping)

        output_df = input_df
        applied_count = 0

        # Apply replace mode mappings (modify source columns in place)
        for source_col, column_mappings in replace_mappings.items():
            case_expr = self._build_case_expression(column_mappings, source_col)

            if case_expr is not None:
                output_df = output_df.withColumn(source_col, case_expr)
                applied_count += len(column_mappings)
                ctx.logger.info(f"Applied {len(column_mappings)} replace mappings to column '{source_col}'")

        # Apply enrich mode mappings (create new column)
        if enrich_mappings:
            # Check if target column already exists
            target_col = self.mapping_ref_group
            if target_col in output_df.columns:
                ctx.logger.warning(f"Column '{target_col}' already exists. Will overwrite with enriched values.")

            # Build combined expression for enrichment
            enrich_expr = self._build_enrich_expression(enrich_mappings)

            if enrich_expr is not None:
                output_df = output_df.withColumn(target_col, enrich_expr)
                applied_count += len(enrich_mappings)
                ctx.logger.info(f"Created enrichment column '{target_col}' with {len(enrich_mappings)} mappings")

        # Handle column renaming for replace mode only
        if self.rename_to_group and replace_mappings:
            # Find the primary source column (first one with mappings)
            primary_source_col = next(iter(replace_mappings.keys()), None)
            if primary_source_col and primary_source_col != self.mapping_ref_group:
                output_df = output_df.withColumnRenamed(primary_source_col, self.mapping_ref_group)
                ctx.logger.info(f"Renamed column '{primary_source_col}' to '{self.mapping_ref_group}'")

        if applied_count > 0:
            ctx.logger.info(f"✅ Applied {applied_count} mappings for group '{self.mapping_ref_group}'")
        else:
            ctx.logger.info(f"No mappings applied for group '{self.mapping_ref_group}'")

        return output_df

    def _build_case_expression(self, mappings: list[Row], source_col: str) -> f.Column | None:
        """Build a CASE WHEN expression for replace mode mappings.

        Args:
            mappings: List of mapping configurations for a column.
            source_col: Name of the source column.

        Returns:
            PySpark column expression or None if no valid mappings.
        """
        case_expr = None

        for mapping in mappings:
            ref_value = mapping["mapping_ref_value"]

            # Determine condition based on mapping type
            if mapping["source_value"] is not None:
                # Simple value mapping
                condition = f.col(source_col) == f.lit(mapping["source_value"])
                self.context.logger.debug(f"Replace mapping: {source_col}='{mapping['source_value']}' -> '{ref_value}'")
            elif mapping["match_expression"] is not None:
                # Expression-based mapping
                try:
                    condition = f.expr(mapping["match_expression"])
                    self.context.logger.debug(f"Replace expression: {mapping['match_expression']} -> '{ref_value}'")
                except Exception as e:
                    self.context.logger.warning(f"Invalid expression '{mapping['match_expression']}': {e!s}, skipping")
                    continue
            else:
                continue

            # Build the CASE WHEN chain
            if case_expr is None:
                case_expr = f.when(condition, f.lit(ref_value))
            else:
                case_expr = case_expr.when(condition, f.lit(ref_value))

        # Add fallback to preserve original values for unmapped cases
        if case_expr is not None:
            case_expr = case_expr.otherwise(f.col(source_col))

        return case_expr

    def _build_enrich_expression(self, mappings: list[Row]) -> f.Column | None:
        """Build a CASE WHEN expression for enrich mode mappings.

        Unlike replace mode, this returns null for unmapped values
        instead of preserving the original value.

        Args:
            mappings: List of mapping configurations for enrichment.

        Returns:
            PySpark column expression or None if no valid mappings.
        """
        case_expr = None

        for mapping in mappings:
            ref_value = mapping["mapping_ref_value"]
            source_col = mapping["source_column"] if "source_column" in mapping else None

            # Determine condition based on mapping type
            if mapping["source_value"] is not None and source_col:
                # Simple value mapping
                condition = f.col(source_col) == f.lit(mapping["source_value"])
                self.context.logger.debug(f"Enrich mapping: {source_col}='{mapping['source_value']}' -> '{ref_value}'")
            elif mapping["match_expression"] is not None:
                # Expression-based mapping
                try:
                    condition = f.expr(mapping["match_expression"])
                    self.context.logger.debug(f"Enrich expression: {mapping['match_expression']} -> '{ref_value}'")
                except Exception as e:
                    self.context.logger.warning(f"Invalid expression '{mapping['match_expression']}': {e!s}, skipping")
                    continue
            else:
                self.context.logger.warning(
                    f"Mapping for '{ref_value}' has neither valid source_value nor match_expression, skipping"
                )
                continue

            # Build the CASE WHEN chain
            if case_expr is None:
                case_expr = f.when(condition, f.lit(ref_value))
            else:
                case_expr = case_expr.when(condition, f.lit(ref_value))

        # For enrichment, unmapped values become null (not preserved)
        # This allows business logic to identify unmapped values
        if case_expr is not None:
            case_expr = case_expr.otherwise(f.lit(None))

        return case_expr


@dataclass(kw_only=True)
class ApplyReferenceMappingsGroupsTask(ETLTask):
    """Apply reference-mapping groups to a DataFrame.

    This task applies multiple mapping groups to enrich a DataFrame
    with canonical reference values. The mode (replace/enrich) is
    determined from the YAML configuration per source column.

    Args:
        mapping_ref_groups: List of mapping groups to apply.
        rename_to_group: If True, renames the source column to mapping_ref_group name (replace mode only).
    """

    mapping_ref_groups: list[str] = field(default_factory=list)
    rename_to_group: bool = False

    task_name: str = field(init=False, default="ApplyReferenceMappingsGroupsTask")

    def execute(self, input_df: DataFrame) -> DataFrame:
        """Enrich the DataFrame with reference mappings.

        Args:
            input_df: Input DataFrame to enrich.

        Returns:
            DataFrame with mapped values.
        """
        ctx = self.context
        df = input_df
        groups = self.mapping_ref_groups

        if not groups:
            ctx.logger.info("No mapping groups specified - skipping")
            return df

        ctx.logger.info(f"Applying reference mappings for groups: {sorted(groups)}")

        for grp in groups:
            df = ApplyReferenceMappingTask(
                context=ctx, mapping_ref_group=grp, rename_to_group=self.rename_to_group
            ).execute(df)

        return df
