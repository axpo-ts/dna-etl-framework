from __future__ import annotations

import argparse
import os
import re
import sys
from pathlib import Path
from typing import Any

import yaml
from IPython import get_ipython

_PLACEHOLDER = re.compile(r"\{\{\s*job\.parameters\.\[(?P<key>[^]]+)]\s*}}")


def _is_interactive() -> bool:
    """Return True if in a notebook/interactive environment."""
    try:
        get_ipython()
        return True
    except NameError:
        return False


def _cli_args() -> dict[str, str]:
    """Parse sys.argv for --key value pairs.

    Returns a dictionary mapping key to value.
    """
    if _is_interactive() or not hasattr(sys, "argv"):
        return {}
    parser = argparse.ArgumentParser(add_help=False)
    # Dynamically add all possible --key arguments as optional
    # (This allows unknown keys to be handled flexibly)
    known_args = [arg for arg in sys.argv[1:] if arg.startswith("--")]
    for arg in known_args:
        parser.add_argument(arg, dest=arg.lstrip("-"), type=str, required=False)
    args, unknown = parser.parse_known_args()
    # Only include keys that were actually passed
    return {k: v for k, v in vars(args).items() if v is not None}


class ConfigLoader:
    """Loads parameters for Databricks jobs and scripts from multiple sources with layered precedence.

    The following table shows how parameters are resolved, from highest to lowest precedence:

    | Layer               | Example Source                       | Precedence      |
    |---------------------|--------------------------------------|-----------------|
    | dbutils widgets     | dbutils.widgets.get("key")           | 1 (highest)     |
    | CLI arguments       | --key value                          | 2               |
    | Environment variable| BUNDLE_VAR_key                       | 3               |
    | Local YAML          | local_dev.yml (section: env)         | 4               |
    | Config file         | config.yaml                          | 5               |
    | Spark conf          | spark.conf.get("key")                | 6               |
    | Job/task parameters | Deployment YAML                      | 7 (lowest)      |

    Args:
        current_file (Path | None): Path to the current Python file (usually Path(__file__).resolve()).
        deployment_file (str | None): Name of the deployment YAML file (e.g., 'my_job_deployment.yml').
        job_key (str | None): Name of the job in the deployment YAML (`resources.jobs.<job_key>`).
        task_key (str | None): Task key in the job (`resources.jobs.<job_key>.tasks.<task_key>`).
        local_yaml (str): Name of the local YAML file for developer overrides. Defaults to 'local_dev.yml'.
        env (str): Section name in the local YAML to use for overrides. Defaults to 'local_dev'.
        config_file (str | None): Path or filename of the dedicated config file (e.g., 'config.yaml').
                                  If a filename is provided, it is resolved with find_file().
        spark (Any): An existing SparkSession. Used to read Spark conf values.
        dbutils (Any | None): Databricks dbutils object. If not provided, attempts to import automatically.
        max_hops (int): Maximum directory levels to search upwards for YAML files. Defaults to 15.

    Raises:
        FileNotFoundError: If the deployment YAML file cannot be found.
        ValueError: If job_key is not provided and multiple jobs exist in the deployment YAML.

    Example:
        >>> loader = ConfigLoader(
        ...     current_file=Path(__file__).resolve(),
        ...     deployment_file="my_job_deployment.yml",
        ...     job_key="my_job",
        ...     task_key="main_task",
        ...     config_file="resources/source/bridge_tool/tagetik/cfg/config.yaml",
        ... )
        >>> params = loader.all()
        >>> print(params["normal_accounts"])  # from config.yaml
    """

    def __init__(
        self,
        *,
        current_file: Path | None = None,  # Path(__file__).resolve()
        deployment_file: str | None = None,  # e.g. "feature_deployment.yml"
        job_key: str | None = None,  # resources.jobs.<job>
        task_key: str | None = None,  # resources.jobs.<job>.tasks.<task_key>
        local_yaml: str = "local_dev.yml",
        env: str = "local_dev",
        config_file: str | None = None,  # e.g. "config.yaml" or "path/to/config.yaml"
        config_section: str | None = None,  # e.g. "my_app"
        spark: Any = None,
        dbutils: Any | None = None,
        max_hops: int = 15,
    ) -> None:
        """Initialize the ConfigLoader.

        Args:
            current_file: Path to the current Python file (usually Path(__file__).resolve()).
            task_file: Path to the current Python file (usually Path(__file__).resolve()).
            deployment_file: Name of the deployment YAML file (e.g., 'my_job_deployment.yml').
            job_key: Name of the job in the deployment YAML (`resources.jobs.<job_key>`).
            task_key: Task key in the job (`resources.jobs.<job_key>.tasks.<task_key>`).
            local_yaml: Name of the local YAML file for developer overrides.
            env: Section name in the local YAML to use for overrides.
            config_file: Path or filename of the dedicated config file (e.g., 'config.yaml').
                         If a filename is provided, it is resolved with find_file().
            config_section: Optional top-level key in config_file to scope values (e.g., 'dev').
            spark: An existing SparkSession. Used to read Spark conf values.
            dbutils: Databricks dbutils object. If not provided, attempts to import automatically.
            max_hops: Maximum directory levels to search upwards for YAML files.
        """
        current_file = Path.cwd() if current_file is None else current_file
        if deployment_file:
            self.deploy_file = self.find_file(deployment_file, current_file, max_hops)
        else:
            self.deploy_file = None

        self.local_yaml = self.find_file(local_yaml, current_file, max_hops)
        self.job_key = job_key
        self.task_key = task_key
        self.env = env
        self.spark = spark

        self.conf_file = self.find_file(config_file, current_file, max_hops) if config_file else None
        self.conf_section = config_section

        try:
            self.dbutils = dbutils or __import__("databricks.sdk.runtime").sdk.runtime.dbutils
        except Exception:
            self.dbutils = None

        if deployment_file and self.deploy_file:
            self._job_params, self._task_params = self._read_deployment()
        else:
            self._job_params, self._task_params = {}, {}

    # ─────────────────── public ───────────────────
    def get(self, key: str, default: str | None = None) -> str | None:
        """Get a configuration parameter by key.

        Args:
            key: The parameter key to look up.
            default: Default value if key is not found.

        Returns:
            The parameter value or default if not found.
        """
        # 1. widgets
        if self.dbutils and hasattr(self.dbutils.widgets, "get"):
            try:
                return self.dbutils.widgets.get(key)
            except Exception:
                pass

        # 2. CLI args
        cli_val = _cli_args().get(key)
        if cli_val is not None:
            return cli_val

        # 3. env
        if val := os.getenv(f"BUNDLE_VAR_{key}"):
            return val

        # 4. local YAML (env-scoped)
        local_val = self._yaml_local().get(key)
        if local_val is not None:
            return local_val

        # 5. dedicated config file
        conf_val = self._yaml_conf().get(key)
        if conf_val is not None:
            return conf_val

        # 6. spark.conf
        if self.spark:
            try:
                return self.spark.conf.get(key)
            except Exception:
                pass

        # 7. job → task defaults
        return self._job_params.get(key, self._task_params.get(key, default))

    def all(self) -> dict[str, str]:
        """Get all configuration parameters as a dictionary.

        Returns:
            Dictionary mapping parameter keys to their values.
        """
        keys = (
            set(self._job_params)
            | set(self._task_params)
            | set(self._yaml_local())
            | set(self._yaml_conf())
            | _safe_widget_keys(self.dbutils)
            | {k.removeprefix("BUNDLE_VAR_") for k in os.environ if k.startswith("BUNDLE_VAR_")}
        )
        if self.spark:
            keys |= {k.split(".")[-1] for k, _ in self.spark.sparkContext.getConf().getAll()}
        return {k: self.get(k) for k in keys}

    # ─────────────────── internal ───────────────────
    @staticmethod
    def find_file(filename: str, start: Path | None = None, max_hops: int = 15) -> Path | None:
        """Search for file with improved logic based on project structure detection.

        Args:
            filename: Name of the file to search for.
            start: Starting directory. Defaults to current working directory.
            max_hops: Maximum number of directory levels to search upwards.

        Returns:
            Path | None: The resolved path to the file if found, otherwise None.
        """
        current_dir = start or Path.cwd()

        # First, try to detect if we're likely in a project root or subfolder
        is_likely_root = ConfigLoader._is_project_root(current_dir)

        if is_likely_root:
            # If we're in the root, search current directory first, then limited downward search
            candidate = current_dir / filename
            if candidate.exists():
                return candidate.resolve()

            # Limited downward search in common directories
            for subdir in ["resources"]:
                search_path = current_dir / subdir
                if search_path.exists():
                    try:
                        for found in search_path.rglob(filename):  # Limited scope
                            return found.resolve()
                    except (OSError, PermissionError):
                        continue
        else:
            # Upward search logic remains the same
            cur = current_dir
            hops = 0
            while cur != cur.parent and hops <= max_hops:
                candidate = cur / filename
                if candidate.exists():
                    return candidate.resolve()
                cur = cur.parent
                hops += 1

        return None

    @staticmethod
    def _is_project_root(path: Path) -> bool:
        """Detect if the current path is likely a project root directory.

        Args:
            path: Path to check

        Returns:
            bool: True if likely a project root, False otherwise
        """
        # Common indicators of project root
        root_indicators = [
            ".git",
            ".gitignore",
            "pyproject.toml",
            "setup.py",
            "requirements.txt",
            "databricks.yml",
            "bundle.yml",
            "Dockerfile",
            "README.md",
            "README.rst",
        ]

        # Check for common project root files/directories
        for indicator in root_indicators:
            if (path / indicator).exists():
                return True

        return False  # Default to assuming we're at not root

    @staticmethod
    def _is_databricks_environment() -> bool:
        """Check if running in Databricks environment."""
        return (
            os.getenv("DATABRICKS_RUNTIME_VERSION") is not None
            or Path("/databricks").exists()
            or Path("/databricks/spark").exists()
        )

    def _read_deployment(self) -> tuple[dict[str, str], dict[str, str]]:
        with self.deploy_file.open() as fh:
            bundle = yaml.safe_load(fh) or {}

        jobs = bundle.get("resources", {}).get("jobs", {})
        if not jobs:
            return {}, {}

        # pick job
        if self.job_key:
            jb = jobs.get(self.job_key)
            if jb is None:
                raise ValueError(f"Job '{self.job_key}' not found")
        elif len(jobs) == 1:
            jb = next(iter(jobs.values()))
        else:
            raise ValueError("Multiple jobs - pass job_key=")

        # --- job-level parameters ------------------------------------
        job_params = {p["name"]: str(p.get("default", "")) for p in jb.get("parameters", [])}

        # --- task-level ----------------------------------------------
        task_params: dict[str, str] = {}
        if jb.get("tasks"):
            tb = _select_task_block(jb["tasks"], self.task_key)
            if tb:
                task_params = _parse_python_task(tb, job_params)

        return job_params, task_params

    def _yaml_local(self) -> dict[str, Any]:
        if not self.local_yaml:
            return {}
        try:
            with self.local_yaml.open() as fh:
                section = (yaml.safe_load(fh) or {}).get(self.env, {})
            # return typed values (not str-cast) to allow lists/dicts in local overrides
            return {k: str(v) for k, v in section.items()}
        except Exception:
            return {}

    def _yaml_conf(self) -> dict[str, Any]:
        """Read the dedicated config file (typed values)."""
        if not self.conf_file:
            return {}
        try:
            with self.conf_file.open(encoding="utf-8") as fh:
                conf = yaml.safe_load(fh) or {}
            if self.conf_section:
                conf = conf.get(self.conf_section, {})
            return conf if isinstance(conf, dict) else {}
        except Exception:
            return {}


# ─────────────────── helpers outside class ───────────────────
def _safe_widget_keys(dbutils: Any) -> set[str]:
    if dbutils and hasattr(getattr(dbutils, "widgets", None), "getAll"):
        try:
            return set(dbutils.widgets.getAll().keys())
        except Exception:
            pass
    return set()


def _select_task_block(task_list: list[dict], task_key: str | None) -> dict | None:
    if task_key:
        return next((t for t in task_list if t.get("task_key") == task_key), None)
    return task_list[0] if len(task_list) == 1 else None


def _parse_python_task(task_block: dict, job_params: dict[str, str]) -> dict[str, str]:
    """Convert a spark_python_task `parameters` list into {arg_name: value}.

    Handles Jinja placeholders like {{job.parameters.[config_path]}}.
    """
    spark_task = task_block.get("spark_python_task", {})
    raw = spark_task.get("parameters", [])
    if not raw:
        # Notebook workflow?
        return task_block.get("notebook_task", {}).get("base_parameters", {})

    # parameters are ['--flag', 'value', '--flag2', 'value2', …]
    pairs = zip(raw[0::2], raw[1::2])
    parsed: dict[str, str] = {}
    for flag, value in pairs:
        key = flag.lstrip("-")
        # resolve {{job.parameters.[xxx]}}
        match = _PLACEHOLDER.fullmatch(value.strip())
        if match:
            ref = match.group("key")
            value = job_params.get(ref, "")
        parsed[key] = value
    return parsed
