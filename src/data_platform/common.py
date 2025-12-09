import json
import logging
import os
import re
import sys
from abc import ABC, abstractmethod
from argparse import ArgumentParser
from typing import Any
from urllib.parse import unquote

import yaml
from pyspark.sql import SparkSession


def get_dbutils(
    spark: SparkSession | None,
) -> Any | None:  # pragma: no cover
    """Get the DBUtils object for interacting with Databricks utilities.

    Args:
        spark (pyspark.sql.SparkSession): The SparkSession object.

    Returns:
        Any: The DBUtils object if it is available, otherwise None.

    """
    try:
        from pyspark.dbutils import DBUtils  # type: ignore

        if "dbutils" not in locals():
            utils = DBUtils(spark)
            return utils
        else:
            return locals().get("dbutils")
    except ImportError:
        return None


def build_config(config_path: dict | str, tag: str = "!ENV", parameters: dict = {}) -> dict:
    """Load a yaml configuration file and resolve any environment variables.

    The environment variables must have !ENV before them and be in this format
    to be parsed: ${VAR_NAME}.
    E.g.:
    database:
        host: !ENV ${HOST}
        port: !ENV ${PORT}
    app:
        log_path: !ENV '/var/${LOG_PATH}'
        something_else: !ENV '${AWESOME_ENV_VAR}/var/${A_SECOND_AWESOME_VAR}'

    Args:
        config_path (str): The path to the yaml file.
        tag (str, optional): The tag to look for. Defaults to "!ENV".
        parameters (Dict, optional): Additional parameters to replace the environment variables. Defaults to {}.

    Returns:
        Dict: The dictionary configuration.

    Raises:
        FileNotFoundError: If the specified config_path does not exist.

    """
    # pattern for global vars: look for ${word}
    pattern = re.compile(".*?\\${(\\w+)}.*?")
    loader = yaml.SafeLoader
    # the tag will be used to mark where to start searching for the pattern
    # e.g. somekey: !ENV somestring${MYENVVAR}blah blah blah
    loader.add_implicit_resolver(tag, pattern, None)

    def constructor_env_variables(loader, node):  # noqa: ANN001 ANN202
        """Constructs environment variables based on the given loader and node.

        Args:
            loader: The loader object.
            node: The current node in the yaml

        Returns:
            str: the parsed string that contains the value of the environment

        """
        value = loader.construct_scalar(node)
        match = pattern.findall(value)  # to find all env variables in line
        if match:
            full_value = value
            for g in match:
                full_value = full_value.replace(
                    f"${{{g}}}",
                    # replace with parameters, if not found, replace with os.environ
                    parameters.get(g, os.environ.get(g, g)),
                )
            return full_value
        return value

    loader.add_constructor(tag, constructor_env_variables)
    if isinstance(config_path, dict):
        # Use safe_dump to preserve quotes and formatting
        yaml_str = yaml.safe_dump(config_path, default_flow_style=False, allow_unicode=True)
        return yaml.load(yaml_str, Loader=loader)
    else:
        with open(config_path) as conf_data:
            return yaml.load(conf_data, Loader=loader)


class Task(ABC):
    """This is an abstract class that provides handy interfaces to implement workloads (e.g. jobs or job tasks).

    Create a child from this class and implement the abstract launch method.
    Class provides access to the following useful objects:
    * self.spark is a SparkSession
    * self.dbutils provides access to the DBUtils
    * self.logger provides access to the Spark-compatible logger
    * self.conf provides access to the parsed configuration of the job

    """

    def __init__(
        self,
        spark: SparkSession | None = None,
        init_conf: dict = {},
        conf_parameter: dict = {},
        logger: logging.Logger | None = None,
    ) -> None:
        """Initializes the Task class."""
        self.spark = self._prepare_spark(spark)
        if logger:
            self.logger = logger
        else:
            self.logger = self._prepare_logger()
        self.dbutils = self.get_dbutils()
        if conf_parameter:
            self.conf_parameter = conf_parameter
        else:
            self.conf_parameter = self._get_conf_file_parameter()
        if init_conf:
            # substituate config values if string !ENV is found within config
            if next(str(value).find("!ENV") for value in init_conf.values()) >= 0:
                self.conf = build_config(config_path=init_conf, parameters=conf_parameter)
            else:
                self.conf = init_conf
        else:
            self.conf = self._provide_config()
        self._log_conf()

    @staticmethod
    def _prepare_spark(spark: SparkSession | None) -> SparkSession:
        if not spark:
            return SparkSession.builder.getOrCreate()
        else:
            return spark

    def get_dbutils(self):  # noqa: ANN201
        """Retrieves the DBUtils instance for the current Spark session.

        Returns:
            DBUtils: The DBUtils instance for the current Spark session.
        """
        utils = get_dbutils(self.spark)

        if not utils:  # pragma: no cover
            self.logger.warn("No DBUtils defined in the runtime")
        else:
            self.logger.info("DBUtils class initialized")

        return utils

    def _provide_config(self) -> dict:
        self.logger.info("Reading configuration from --conf-file job option")
        conf_file = self._get_conf_file()
        if not conf_file:
            self.logger.info(
                "No conf file was provided, setting configuration to empty dict."
                "Please override configuration in subclass init method"
            )
            return {}
        else:
            self.logger.info(f"Conf file was provided, reading configuration from {conf_file}")
            return self._read_config(conf_file=conf_file, conf_parameter=self.conf_parameter)

    @staticmethod
    def _get_conf_file() -> str:
        p = ArgumentParser()
        p.add_argument("--conf-file", required=False, type=str)
        namespace = p.parse_known_args(sys.argv[1:])[0]
        return namespace.conf_file

    @staticmethod
    def _get_conf_file_parameter() -> dict:
        p = ArgumentParser()
        p.add_argument("--conf-file-parameter", required=False, type=str)
        namespace = p.parse_known_args(sys.argv[2:])[0]
        if namespace.conf_file_parameter is not None:
            return json.loads(unquote(namespace.conf_file_parameter))
        return {}

    @staticmethod
    def _read_config(conf_file: str, conf_parameter: dict = {}) -> dict[str, Any]:
        config = build_config(config_path=conf_file, parameters=conf_parameter)
        return config

    def _prepare_logger(self) -> logging.Logger:
        logging.basicConfig(
            level=logging.INFO,
            format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
            datefmt="%Y-%m-%d %H:%M:%S",
        )
        logger = logging.getLogger(self.__class__.__name__)

        return logger

    def _log_conf(self) -> None:
        # log parameters
        self.logger.info("Launching job with configuration parameters:")
        for key, item in self.conf.items():
            self.logger.info(f"\t Parameter: {key} with value => {item}")

    @abstractmethod
    def launch(self) -> None:  # pragma: no cover
        """Main method of the job."""
        pass


def build_config_json(config_path: dict | str, parameters: dict = {}) -> dict:
    """Load a JSON configuration file and resolve any environment variables.

    The environment variables must be in this format to be parsed: ${VAR_NAME}.
    E.g.:
    {
        "reader_options": {
            "path": "/Volumes/${catalog_prefix}bronze/${schema_prefix}data_governance"
        }
    }

    Args:
        config_path (dict | str): The path to the JSON file or a JSON dictionary.
        parameters (dict, optional): Additional parameters to replace the environment variables.

    Returns:
        dict: The resolved configuration dictionary.

    Raises:
        FileNotFoundError: If the specified config_path does not exist (for file paths).

    """
    # Pattern to find variables in the format ${VAR_NAME}
    pattern = re.compile(r"\${(\w+)}")

    def resolve_env_variables(value: Any) -> Any:
        """Resolve environment variables in the given value."""
        if isinstance(value, str):
            # Replace all occurrences of ${VAR_NAME} with the corresponding value
            matches = pattern.findall(value)
            for match in matches:
                value = value.replace(f"${{{match}}}", parameters.get(match, os.environ.get(match, match)))
            return value
        elif isinstance(value, dict):
            # Recursively resolve in dictionaries
            return {k: resolve_env_variables(v) for k, v in value.items()}
        elif isinstance(value, list):
            # Recursively resolve in lists
            return [resolve_env_variables(item) for item in value]
        return value

    if isinstance(config_path, dict):
        # If a dictionary is passed, process directly
        return resolve_env_variables(config_path)
    else:
        # If a file path is passed, load the JSON file
        if not os.path.exists(config_path):
            raise FileNotFoundError(f"Configuration file not found: {config_path}")
        with open(config_path) as conf_file:
            config_data = json.load(conf_file)
            return resolve_env_variables(config_data)
