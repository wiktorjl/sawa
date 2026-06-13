"""MCP server monitoring: per-call timing + consecutive-failure alerts.

This module owns:

* file logging setup for the server (separate from the stdio-targeted
  stderr handler, so log lines survive the MCP client's pipe),
* a per-tool consecutive-failure counter that fires an NTFY alert when a
  given tool fails ``MCP_FAILURE_ALERT_THRESHOLD`` times in a row.

The counter resets to zero on the first success for that tool, and resets
again after firing an alert so a persistently-broken tool fires at most
once per N consecutive failures rather than spamming.

The MCP server is an ephemeral stdio process (often one process per call),
so an in-memory counter would almost never reach the alert threshold. The
counts are therefore persisted to a small JSON file next to the audit logs
so consecutive failures accumulate *across* short-lived processes.
"""

from __future__ import annotations

import json
import logging
import os
from logging.handlers import TimedRotatingFileHandler
from pathlib import Path

from sawa.utils.notify import NotificationLevel, get_notifier

_DEFAULT_LOG_DIR = Path.home() / ".sawa" / "logs"
_FAILURE_ALERT_THRESHOLD = int(os.environ.get("MCP_FAILURE_ALERT_THRESHOLD", "3"))


def _state_file() -> Path:
    """Path to the cross-process failure-counter store.

    Resolved on each access so tests (and the audit log) can redirect it via
    ``MCP_LOG_DIR`` without re-importing the module.
    """
    log_dir = Path(os.environ.get("MCP_LOG_DIR") or _DEFAULT_LOG_DIR)
    return log_dir / "mcp_failure_counts.json"


def _load_counts() -> dict[str, int]:
    """Load persisted per-tool failure counts; empty dict on any problem."""
    try:
        with open(_state_file(), encoding="utf-8") as f:
            data = json.load(f)
    except (OSError, ValueError):
        return {}
    if not isinstance(data, dict):
        return {}
    # Coerce defensively: a hand-edited/corrupt file should not crash a tool.
    return {str(k): int(v) for k, v in data.items() if isinstance(v, (int, float))}


def _save_counts(counts: dict[str, int]) -> None:
    """Persist failure counts; best-effort so monitoring never breaks a call."""
    path = _state_file()
    try:
        path.parent.mkdir(parents=True, exist_ok=True)
        with open(path, "w", encoding="utf-8") as f:
            json.dump(counts, f)
    except OSError:
        # Auditing/alerting is best-effort; never let it surface to the caller.
        pass


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
    counts = _load_counts()

    if success:
        logger.info("[mcp] tool=%s duration_ms=%.1f status=ok", tool, duration_ms)
        if counts.get(tool):
            # Recovered after some failures — reset and emit a recovery line.
            previous = counts.pop(tool)
            logger.info("[mcp] tool=%s recovered after %d failure(s)", tool, previous)
            _save_counts(counts)
        return

    err_type = type(error).__name__ if error else "Unknown"
    count = counts.get(tool, 0) + 1
    counts[tool] = count
    # Always log the failure at WARNING with enough context to act on, even
    # for the first occurrence — the cross-process count makes this useful
    # even when each call runs in its own ephemeral process.
    logger.warning(
        "[mcp] tool=%s duration_ms=%.1f status=error error_type=%s "
        "consecutive_failures=%d error=%s",
        tool,
        duration_ms,
        err_type,
        count,
        error,
    )

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
        counts[tool] = 0

    _save_counts(counts)


def reset_counters() -> None:
    """Clear all persisted failure counters (test helper)."""
    try:
        _state_file().unlink()
    except OSError:
        pass
