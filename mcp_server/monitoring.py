"""MCP server monitoring: per-call timing + consecutive-failure alerts.

This module owns:

* file logging setup for the server (separate from the stdio-targeted
  stderr handler, so log lines survive the MCP client's pipe),
* a per-tool consecutive-failure counter that fires an NTFY alert when a
  given tool fails ``MCP_FAILURE_ALERT_THRESHOLD`` times in a row.

The counter resets to zero on the first success for that tool, and resets
again after firing an alert so a persistently-broken tool fires at most
once per N consecutive failures rather than spamming.
"""

from __future__ import annotations

import logging
import os
from collections import defaultdict
from logging.handlers import TimedRotatingFileHandler
from pathlib import Path

from sawa.utils.notify import NotificationLevel, get_notifier

_DEFAULT_LOG_DIR = Path.home() / ".sawa" / "logs"
_FAILURE_ALERT_THRESHOLD = int(os.environ.get("MCP_FAILURE_ALERT_THRESHOLD", "3"))

_failure_counts: dict[str, int] = defaultdict(int)


def configure_file_logging(logger: logging.Logger) -> Path | None:
    """Attach a rotating file handler to the MCP root logger.

    Returns the resolved log file path so the caller can mention it on
    startup. ``None`` if the directory could not be created.
    """
    log_dir = Path(os.environ.get("MCP_LOG_DIR") or _DEFAULT_LOG_DIR)
    try:
        log_dir.mkdir(parents=True, exist_ok=True)
    except OSError as exc:
        logger.warning("Could not create MCP log dir %s: %s", log_dir, exc)
        return None

    log_file = log_dir / "mcp.log"
    handler = TimedRotatingFileHandler(
        log_file,
        when="midnight",
        backupCount=14,
        encoding="utf-8",
    )
    handler.setLevel(logging.DEBUG)
    handler.setFormatter(
        logging.Formatter(
            "%(asctime)s [%(levelname)s] %(name)s: %(message)s",
            datefmt="%Y-%m-%d %H:%M:%S",
        )
    )
    # Attach to the root logger so child loggers (mcp_server.*, sawa.*) all
    # flow into the same file without each having to opt in.
    logging.getLogger().addHandler(handler)
    return log_file


def record_call_outcome(
    tool: str,
    *,
    success: bool,
    duration_ms: float,
    logger: logging.Logger,
    error: BaseException | None = None,
) -> None:
    """Log a structured outcome line and, on failure, maybe fire an alert.

    Called from ``call_tool`` at the success and failure boundaries.
    """
    if success:
        logger.info("[mcp] tool=%s duration_ms=%.1f status=ok", tool, duration_ms)
        if _failure_counts.get(tool):
            # Recovered after some failures — reset and emit a recovery line.
            previous = _failure_counts.pop(tool)
            logger.info("[mcp] tool=%s recovered after %d failure(s)", tool, previous)
        return

    err_type = type(error).__name__ if error else "Unknown"
    logger.warning(
        "[mcp] tool=%s duration_ms=%.1f status=error error_type=%s",
        tool,
        duration_ms,
        err_type,
    )

    _failure_counts[tool] += 1
    count = _failure_counts[tool]
    if count >= _FAILURE_ALERT_THRESHOLD:
        get_notifier(logger).send(
            title=f"Sawa MCP: {tool} failing",
            body=(
                f"Tool '{tool}' has failed {count} consecutive times.\n"
                f"Latest error: {err_type}: {error}\n\n"
                "Counter will reset after the next success or after this alert."
            ),
            level=NotificationLevel.WARNING,
            tags=["warning", "mcp", tool],
        )
        # Reset so we alert again only on the next streak of failures.
        _failure_counts[tool] = 0


def reset_counters() -> None:
    """Clear all in-memory failure counters (test helper)."""
    _failure_counts.clear()
