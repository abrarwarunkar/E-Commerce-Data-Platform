"""
Centralized logging configuration for the E-Commerce Data Platform.
Data Lineage: configs/logging_config.py -> all platform modules
"""

import logging
import logging.handlers
import os
import sys
from pathlib import Path


LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO").upper()
LOG_DIR = Path(os.getenv("LOG_DIR", "/app/logs"))


class ColoredFormatter(logging.Formatter):
    """Custom formatter adding colors for console output."""

    COLORS = {
        "DEBUG": "\033[36m",    # Cyan
        "INFO": "\033[32m",     # Green
        "WARNING": "\033[33m",  # Yellow
        "ERROR": "\033[31m",    # Red
        "CRITICAL": "\033[35m", # Magenta
        "RESET": "\033[0m",
    }

    def format(self, record: logging.LogRecord) -> str:
        color = self.COLORS.get(record.levelname, self.COLORS["RESET"])
        reset = self.COLORS["RESET"]
        record.levelname = f"{color}{record.levelname}{reset}"
        return super().format(record)


def get_logger(name: str, log_file: str | None = None) -> logging.Logger:
    """
    Create and return a configured logger instance.

    Args:
        name: Logger name (typically __name__ of the calling module)
        log_file: Optional log file name (relative to LOG_DIR)

    Returns:
        Configured Logger instance
    """
    logger = logging.getLogger(name)

    if logger.handlers:
        return logger  # Avoid duplicate handlers

    logger.setLevel(getattr(logging, LOG_LEVEL, logging.INFO))

    # Console handler
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setLevel(logging.DEBUG)
    console_formatter = ColoredFormatter(
        fmt="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )
    console_handler.setFormatter(console_formatter)
    logger.addHandler(console_handler)

    # File handler (rotating)
    if log_file:
        LOG_DIR.mkdir(parents=True, exist_ok=True)
        file_path = LOG_DIR / log_file
        file_handler = logging.handlers.RotatingFileHandler(
            file_path,
            maxBytes=10 * 1024 * 1024,  # 10MB
            backupCount=5,
            encoding="utf-8",
        )
        file_handler.setLevel(logging.DEBUG)
        file_formatter = logging.Formatter(
            fmt="%(asctime)s | %(levelname)s | %(name)s | %(funcName)s:%(lineno)d | %(message)s",
            datefmt="%Y-%m-%d %H:%M:%S",
        )
        file_handler.setFormatter(file_formatter)
        logger.addHandler(file_handler)

    logger.propagate = False
    return logger
