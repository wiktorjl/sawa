"""Unified logging configuration."""

import logging
import sys
import traceback
from datetime import datetime
from io import TextIOWrapper
from logging.handlers import RotatingFileHandler
from pathlib import Path
from typing import TextIO

from sawa.utils.security import (
    ensure_private_directory,
    ensure_private_file,
    open_private_text,
    redact_sensitive_text,
)

# Default log location: ~/.sawa/logs. Matches the convention used by
# scripts/market_scheduler.sh (~/.sawa/scheduler) so all sawa state lives
# under one root.
DEFAULT_LOG_DIR = Path.home() / ".sawa" / "logs"

# Per-file rotation cap. With 5 backups this caps any single log family at
# ~150 MB on disk.
LOG_FILE_MAX_BYTES = 25 * 1024 * 1024
LOG_FILE_BACKUP_COUNT = 5


class _PrivateRotatingFileHandler(RotatingFileHandler):
    """Rotating handler whose newly-created base file is always mode 0600."""

    def _open(self) -> TextIOWrapper:
        return open_private_text(Path(self.baseFilename), self.mode)


class RedactingFormatter(logging.Formatter):
    """Formatter that removes credentials from fully-rendered log messages."""

    def format(self, record: logging.LogRecord) -> str:
        return redact_sensitive_text(super().format(record))


class RedactingFilter(logging.Filter):
    """Sanitize a record before any pre-existing handler can render it."""

    def filter(self, record: logging.LogRecord) -> bool:
        record.msg = redact_sensitive_text(record.getMessage())
        record.args = ()
        if record.exc_info:
            record.exc_text = redact_sensitive_text(
                "".join(traceback.format_exception(*record.exc_info))
            )
            record.exc_info = None
        if record.stack_info:
            record.stack_info = redact_sensitive_text(record.stack_info)
        return True


def install_redaction_filters(logger: logging.Logger | None = None) -> None:
    """Attach credential redaction to every handler on ``logger``."""
    target = logger or logging.getLogger()
    for handler in target.handlers:
        if not any(isinstance(item, RedactingFilter) for item in handler.filters):
            handler.addFilter(RedactingFilter())


def get_default_log_dir() -> Path:
    """Return the default log directory, creating it if necessary."""
    return ensure_private_directory(DEFAULT_LOG_DIR)


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

    formatter = RedactingFormatter(log_format, date_format)
    stream_handler = logging.StreamHandler(stream)
    stream_handler.setFormatter(formatter)
    stream_handler.addFilter(RedactingFilter())
    handlers: list[logging.Handler] = [stream_handler]

    # Resolve log_dir: None → default XDG-style path; falsy string → off.
    if log_dir is None:
        resolved_dir: Path | None = get_default_log_dir()
    elif log_dir == "" or log_dir is False:  # noqa: E712
        resolved_dir = None
    else:
        resolved_dir = Path(log_dir)

    if resolved_dir is not None:
        ensure_private_directory(resolved_dir)
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        log_file = resolved_dir / f"{run_name}_{timestamp}.log"
        ensure_private_file(log_file)
        file_handler = _PrivateRotatingFileHandler(
            log_file,
            maxBytes=LOG_FILE_MAX_BYTES,
            backupCount=LOG_FILE_BACKUP_COUNT,
            encoding="utf-8",
        )
        # The base file is pre-created privately above. Rotated backups inherit
        # that mode via rename; enforce it again in case an older file existed.
        log_file.chmod(0o600)
        file_handler.setLevel(logging.DEBUG)  # File always gets DEBUG
        file_handler.setFormatter(formatter)
        file_handler.addFilter(RedactingFilter())
        handlers.append(file_handler)

    logging.basicConfig(
        level=log_level,
        handlers=handlers,
    )
    # basicConfig is a no-op when an embedding process already configured the
    # root logger. In that case, sanitize those existing handlers too.
    install_redaction_filters()
    return logging.getLogger(name or __name__)
