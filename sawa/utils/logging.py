"""Unified logging configuration."""

import logging
import sys
from typing import TextIO


def setup_logging(
    verbose: bool = False,
    name: str | None = None,
    stream: TextIO = sys.stdout,
) -> logging.Logger:
    """
    Configure logging with timestamps and appropriate level.

    Args:
        verbose: Enable DEBUG level if True, otherwise INFO
        name: Logger name (defaults to caller's module name)
        stream: Output stream (defaults to stdout)

    Returns:
        Configured logger instance
    """
    log_level = logging.DEBUG if verbose else logging.INFO
    logging.basicConfig(
        level=log_level,
        format="%(asctime)s [%(levelname)s] %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
        handlers=[logging.StreamHandler(stream)],
    )
    return logging.getLogger(name or __name__)
