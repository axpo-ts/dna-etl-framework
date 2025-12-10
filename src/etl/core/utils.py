from typing import Any

from databricks.sdk import WorkspaceClient


def get_databricks_environment() -> str:
    """Determine environment (PROD or NON_PROD) based on workspace ID."""
    workspace_client = WorkspaceClient()
    workspace_id = workspace_client.get_workspace_id()

    return "PROD" if workspace_id == 2433690965938898 else "NON_PROD"


def loganalytics_conn_str(dbutils: Any, env: str) -> str | None:
    """Retrieve the Azure Log Analytics connection string based on environment.

    Args:
        env: Current environment ('PROD' or other).
        dbutils: Databricks utilities object for secret management.

    Returns:
        The connection string if in PROD environment; otherwise, None.
    """
    if env == "PROD":
        return dbutils.secrets.get("dna_prod_secretscope", "loganalytics-connection-string")  # type: ignore
    return None
