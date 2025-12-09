from data_platform.etl.core.task_context import TaskContext


def path_exists(context: TaskContext, path: str) -> bool:
    """Check if a path exists using Databricks utilities.

    Args:
        context: Task context containing Databricks utilities instance.
        path (str): The path to check.

    Returns:
        bool: True if path exists, False otherwise.
    """
    try:
        context.dbutils.fs.ls(path)
        return True
    except Exception:
        return False


def list_folders_in_path(context: TaskContext, path: str) -> list[str]:
    """List directories in a given path using Databricks utilities.

    Args:
        context: Task context containing Databricks utilities instance.
        path (str): The path to list files from.

    Returns:
        list[str]: List of directory paths (not just names) in the path.
    """
    try:
        file_info = context.dbutils.fs.ls(path)
        folders = [f.path for f in file_info]
        context.logger.info(f"Folders found in {path}: {[f.split('/')[-2] for f in folders]}")
        return folders
    except Exception as e:
        context.logger.warning(f"Could not list folders in {path}: {e!s}")
        return []
