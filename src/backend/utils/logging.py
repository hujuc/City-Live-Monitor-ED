"""Logging configuration using loguru."""

import sys
from pathlib import Path

from loguru import logger


def setup_logging(log_level: str = "INFO", log_file: Path | None = None) -> None:
    """Configure loguru with console + optional file sink."""
    logger.remove()
    logger.add(sys.stderr, level=log_level, colorize=True,
               format="<green>{time:YYYY-MM-DD HH:mm:ss}</green> | <level>{level: <8}</level> | {message}")
    if log_file:
        log_file.parent.mkdir(parents=True, exist_ok=True)
        logger.add(str(log_file), level=log_level, rotation="10 MB", retention="7 days",
                   format="{time:YYYY-MM-DD HH:mm:ss} | {level: <8} | {message}")
