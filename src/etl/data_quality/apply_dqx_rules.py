import json
from dataclasses import dataclass

from databricks.labs.dqx.engine import DQEngine  # type: ignore
from databricks.sdk import WorkspaceClient  # type: ignore
from pyspark.sql import DataFrame
from pyspark.sql import functions as f

from etl.transform import transform as transform_utils

from ..etl_task import ETLTask


@dataclass
class ApplyDqxRulesTask(ETLTask):
    """Applies data quality rules to a DataFrame using DQX engine.

    Validates data against configured rules, quarantines failing records,
    and generates metrics for monitoring data quality.
    """

    source_df: DataFrame
    # TODO change this for the TableIdentifier
    target_schema_name: str
    target_table_name: str
    dq_metrics_table: str
    dq_quarantine_table: str
    alerts_url: str | None = None
    dq_rules_file_path: str | None = None
    dq_rules_table: str | None = None

    task_name = "ApplyDqxRulesTask"

    def execute(self) -> DataFrame:
        """Execute data quality validation on the source DataFrame.

        Returns:
            DataFrame containing only records that passed all quality checks.
        """
        self.context.logger.info("get the input dataframe")

        rules_config = self.get_dataquality_rules()
        self.context.logger.info(f"rules_config: {rules_config}")

        # if no path is provided then skip DQ
        if rules_config is None or len(rules_config) == 0:
            msg = "No data quality rule available for the schema and table, skipping this step."
            self.context.logger.info(msg)

            return self.source_df

        checks = self.create_checks(rules_config)
        self.context.logger.info(f"checks: {checks}")
        dq_engine = DQEngine(WorkspaceClient())

        # apply quality rules on the dataframe and report issues as additional columns
        valid_df, quarantined_df = dq_engine.apply_checks_by_metadata_and_split(self.source_df, checks)
        final_counts_df = self.calculate_final_counts(quarantined_df, rules_config, valid_df)
        self.write_to_metrics_table(final_counts_df)
        self.write_to_quarantine_table(quarantined_df)
        self.context.logger.info("DQ rules applied to dataframe")

        # add dataframe onto task context
        self.context.logger.info("return dataframe")
        return valid_df

    def get_dataquality_rules(self) -> list:
        """Retrieve data quality rules from file or table.

        Returns:
            List of rules filtered for target schema and table.
        """
        if self.dq_rules_file_path is not None:
            with open(self.dq_rules_file_path) as file:
                rules_config_file = json.loads(file.read())

            rules_config = [
                rule
                for rule in rules_config_file
                if rule.get("table") == self.target_table_name and rule.get("schema") == self.target_schema_name
            ]
        else:
            if not self.context.spark.catalog.tableExists(self.dq_rules_table):
                self.context.logger.warning(
                    f"DQ rules table '{self.dq_rules_table}' does not exist. Returning empty rules config."
                )
                return []
            # Fetch only enabled rules from the table
            rules_config_df = (
                self.context.spark.read.table(self.dq_rules_table)
                .filter("enabled = true")
                .select("table", "schema", "constraint", "name")
            )

            filtered_rules_config_df = rules_config_df.filter(
                (f.lower(f.trim(f.col("table"))) == self.target_table_name.lower().strip())
                & (f.lower(f.trim(f.col("schema"))) == self.target_schema_name.lower().strip())
            )

            rules_config = [row.asDict() for row in filtered_rules_config_df.collect()]

        return rules_config

    def create_checks(self, rules_config: list) -> list:
        """Convert rules configuration to DQX check format.

        Args:
            rules_config: List of rule dictionaries with 'constraint' and 'name' keys.

        Returns:
            List of DQX-compatible check dictionaries.
        """
        checks = []
        for rule in rules_config:
            # Normalize keys by converting to lowercase
            lower_rule = {k.lower(): v for k, v in rule.items()}
            constraint = lower_rule.get("constraint")
            name = lower_rule.get("name")

            check = {
                "criticality": "warn",
                "check": {"function": "sql_expression", "arguments": {"expression": constraint, "msg": name}},
            }
            checks.append(check)
        return checks

    def calculate_final_counts(self, quarantined_df: DataFrame, rules_config: list, valid_df: DataFrame) -> DataFrame:
        """Calculate passing and failing record counts for each rule.

        Args:
            quarantined_df: DataFrame with failed records.
            rules_config: List of applied rules.
            valid_df: DataFrame with valid records.

        Returns:
            DataFrame with counts per rule and metadata.
        """
        valid_count = valid_df.count()
        self.context.logger.info(f"valid rows count: {valid_count}")
        normalized_rules = [{k.lower(): v for k, v in rule.items()} for rule in rules_config]

        if quarantined_df.isEmpty():
            # Prepare final counts for all rules
            final_counts = [
                (self.target_table_name, self.target_schema_name, rule.get("name"), valid_count, 0)
                for rule in normalized_rules
            ]
        else:
            failed_df = quarantined_df.select(f.explode("_warnings").alias("warning")).select(
                f.col("warning.message").alias("rule_name")
            )

            failed_counts = failed_df.groupBy("rule_name").agg(f.count("*").alias("Failing_Count")).collect()

            failed_count_dict = {row.rule_name: row.Failing_Count for row in failed_counts}

            # Prepare the results using a list comprehension
            final_counts = [
                (
                    self.target_table_name,
                    self.target_schema_name,
                    rule.get("name"),
                    valid_count - failed_count_dict.get(rule.get("name"), 0),
                    failed_count_dict.get(rule.get("name"), 0),
                )
                for rule in normalized_rules
            ]

        final_counts_df = self.context.spark.createDataFrame(
            final_counts, ["table", "schema", "name", "passing_records", "failing_records"]
        )

        # Add a Total_Records column to the DataFrame
        final_counts_df = (
            final_counts_df.withColumn("total_number_of_records", f.col("passing_records") + f.col("failing_records"))
            .withColumn("timestamp", f.lit(f.current_timestamp()))
            .withColumn("evaluation", f.date_format(f.col("timestamp"), "yyyyMMdd_HHmm"))
        )

        return final_counts_df

    def write_to_quarantine_table(self, df: DataFrame) -> None:
        """Write failed records to quarantine table as JSON.

        Args:
            df: DataFrame containing quarantined records.
        """
        try:
            if df.isEmpty():
                self.context.logger.info("No quarantined data to write to the table.")
                return
            exclude_cols = ["_errors", "_warnings"]
            include_cols = [col for col in df.columns if col not in exclude_cols]

            excluded_json_cols = [
                f.to_json(f.col(col), options={"ignoreNullFields": "false"}).alias(col)
                for col in exclude_cols
                if col in df.columns  # Only include if column exists
            ]

            json_cols = f.to_json(
                f.struct(*[f.col(col) for col in include_cols]), options={"ignoreNullFields": "false"}
            ).alias("json_data")

            json_df = df.select(json_cols, *excluded_json_cols)

            json_df = json_df.drop(*[col for col in ["schema", "table"] if col in json_df.columns])

            json_df = (
                json_df.withColumn("table", f.lit(self.target_table_name))
                .withColumn("schema", f.lit(self.target_schema_name))
                .withColumnRenamed("_errors", "errors")
                .withColumnRenamed("_warnings", "warnings")
            )

            json_df = json_df.select("schema", "table", "json_data", "errors", "warnings")

            # Add timestamps and user info
            json_df = transform_utils.add_audit_cols_snake_case(json_df, self.context.logger)

            if not self.context.spark.catalog.tableExists(self.dq_quarantine_table):
                self.context.logger.warning(
                    f"DQ quarantine table '{self.dq_quarantine_table}' does not exist, skipping write."
                )
                return

            # Write to the quarantine table
            json_df.write.mode("append").saveAsTable(self.dq_quarantine_table)

        except Exception as e:
            self.context.logger.error(f"Failed to write DataFrame to table: {e}")
            raise

    def write_to_metrics_table(self, df: DataFrame) -> None:
        """Write quality metrics to monitoring table.

        Args:
            df: DataFrame containing calculated metrics.
        """
        try:
            df = transform_utils.add_audit_cols_snake_case(df, self.context.logger)
            df = transform_utils.generate_surrogate_hash_key(
                df, self.context.logger, ["schema", "table", "name", "evaluation"], "table_id"
            )
            if not self.context.spark.catalog.tableExists(self.dq_metrics_table):
                self.context.logger.warning(
                    f"DQ metrics table '{self.dq_metrics_table}' does not exist, skipping writing metrics."
                )
                return
            df.write.mode("append").saveAsTable(self.dq_metrics_table)
        except Exception as e:
            self.context.logger.error(f"Failed to write DataFrame to table: {e}")
            raise Exception("Failed to write DataFrame to table.")
