import logging
import sys
from config.config import config
from typing import Optional

def setup_logger(name: Optional[str] = None) -> logging.Logger:
    """
    Sets up and returns a configured logger instance.

    Args:
        name: Logger name (typically __name__ from the calling module)

    Returns:
        Configured logger instance
    """
    logger = logging.getLogger(name or __name__)

    # Avoid adding multiple handlers if logger already exists
    if logger.handlers:
        return logger

    # Set log level from config
    log_level = getattr(logging, config.LOG_LEVEL.upper(), logging.INFO)
    logger.setLevel(log_level)

    # Create console handler
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setLevel(log_level)

    # Create formatter
    formatter = logging.Formatter(
        "%(asctime)s - %(name)s - %(levelname)s - %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )
    console_handler.setFormatter(formatter)

    # Add handler to logger
    logger.addHandler(console_handler)

    return logger


# Create a default logger instance
logger = setup_logger()
