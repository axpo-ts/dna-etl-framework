import logging


def get_logger(name: str) -> logging.Logger:
    """Get a logger instance with a specific name.

    Args:
        name (str): The name of the logger.

    Returns:
        logging.Logger: A logger instance.
    """
    logger = logging.getLogger(name)
    logger.setLevel(logging.INFO)  # or INFO, WARNING, etc.

    # Create a handler that writes to stdout (Databricks will capture this)
    handler = logging.StreamHandler()
    handler.setLevel(logging.INFO)

    # Define a formatter and add it to the handler
    formatter = logging.Formatter("%(asctime)s - %(levelname)s - %(message)s")
    handler.setFormatter(formatter)

    # Add the handler to the logger (only once)
    if not logger.hasHandlers():
        logger.addHandler(handler)
    return logger
