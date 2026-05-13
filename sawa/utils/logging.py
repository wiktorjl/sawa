"""Unified logging configuration."""

import logging
import sys
from datetime import datetime
from logging.handlers import RotatingFileHandler
from pathlib import Path
from typing import TextIO

# Default log location: ~/.sawa/logs. Matches the convention used by
# scripts/market_scheduler.sh (~/.sawa/scheduler) so all sawa state lives
# under one root.
DEFAULT_LOG_DIR = Path.home() / ".sawa" / "logs"

# Per-file rotation cap. With 5 backups this caps any single log family at
# ~150 MB on disk.
LOG_FILE_MAX_BYTES = 25 * 1024 * 1024
LOG_FILE_BACKUP_COUNT = 5


def get_default_log_dir() -> Path:
    """Return the default log directory, creating it if necessary."""
    DEFAULT_LOG_DIR.mkdir(parents=True, exist_ok=True)
    return DEFAULT_LOG_DIR


def setup_logging(
    verbose: bool = False,
    name: str | None = None,
    stream: TextIO = sys.stdout,
    log_dir: Path | str | None = None,
    run_name: str = "sawa",
) -> logging.Logger:
    """
    Configure logging with timestamps and appropriate level.

    Args:
        verbose: Enable DEBUG level if True, otherwise INFO.
        name: Logger name (defaults to caller's module name).
        stream: Output stream (defaults to stdout).
        log_dir: Directory for log files. ``None`` (default) routes to
            ``~/.sawa/logs/``. Pass ``False``-y string or ``""`` to disable
            file logging.
        run_name: Prefix for log file name (default: "sawa").

    Returns:
        Configured logger instance.

    File logs are written to ``<log_dir>/<run_name>_<YYYYMMDD_HHMMSS>.log``.
    They use a ``RotatingFileHandler`` capped at 25 MB × 5 backups, so a single
    long-running run can't fill the disk like the historical 100+ MB intraday
    logs.
    """
    log_level = logging.DEBUG if verbose else logging.INFO
    log_format = "%(asctime)s [%(levelname)s] %(message)s"
    date_format = "%Y-%m-%d %H:%M:%S"

    handlers: list[logging.Handler] = [logging.StreamHandler(stream)]

    # Resolve log_dir: None → default XDG-style path; falsy string → off.
    if log_dir is None:
        resolved_dir: Path | None = get_default_log_dir()
    elif log_dir == "" or log_dir is False:  # noqa: E712
        resolved_dir = None
    else:
        resolved_dir = Path(log_dir)

    if resolved_dir is not None:
        resolved_dir.mkdir(parents=True, exist_ok=True)
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        log_file = resolved_dir / f"{run_name}_{timestamp}.log"
        file_handler = RotatingFileHandler(
            log_file,
            maxBytes=LOG_FILE_MAX_BYTES,
            backupCount=LOG_FILE_BACKUP_COUNT,
            encoding="utf-8",
        )
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
