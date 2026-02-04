"""Unified logging configuration."""

import logging
import sys
from datetime import datetime
from pathlib import Path
from typing import TextIO


def setup_logging(
    verbose: bool = False,
    name: str | None = None,
    stream: TextIO = sys.stdout,
    log_dir: Path | None = None,
    run_name: str = "sawa",
) -> logging.Logger:
    """
    Configure logging with timestamps and appropriate level.

    Args:
        verbose: Enable DEBUG level if True, otherwise INFO
        name: Logger name (defaults to caller's module name)
        stream: Output stream (defaults to stdout)
        log_dir: Directory for log files (if None, no file logging)
        run_name: Prefix for log file name (default: "sawa")

    Returns:
        Configured logger instance
    """
    log_level = logging.DEBUG if verbose else logging.INFO
    log_format = "%(asctime)s [%(levelname)s] %(message)s"
    date_format = "%Y-%m-%d %H:%M:%S"

    handlers: list[logging.Handler] = [logging.StreamHandler(stream)]

    # Add file handler if log_dir specified
    if log_dir:
        log_dir = Path(log_dir)
        log_dir.mkdir(parents=True, exist_ok=True)
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        log_file = log_dir / f"{run_name}_{timestamp}.log"
        file_handler = logging.FileHandler(log_file, encoding="utf-8")
        file_handler.setLevel(logging.DEBUG)  # File always gets DEBUG
        file_handler.setFormatter(logging.Formatter(log_format, date_format))
        handlers.append(file_handler)

    logging.basicConfig(
        level=log_level,
        format=log_format,
        datefmt=date_format,
        handlers=handlers,
    )
    return logging.getLogger(name or __name__)
