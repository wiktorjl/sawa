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
import math
import os
import threading
import time
import uuid
from collections.abc import Iterator
from contextlib import contextmanager
from io import TextIOWrapper
from logging.handlers import TimedRotatingFileHandler
from pathlib import Path
from typing import Any

from sawa.utils.logging import RedactingFilter, RedactingFormatter
from sawa.utils.notify import NotificationLevel, get_notifier
from sawa.utils.security import (
    ensure_private_directory,
    ensure_private_file,
    open_private_text,
    redact_sensitive_text,
)

try:  # POSIX cross-process file locking.
    import fcntl as _fcntl
except ImportError:  # pragma: no cover - exercised on Windows
    _fcntl = None  # type: ignore[assignment]

try:  # Windows cross-process file locking.
    import msvcrt as _msvcrt
except ImportError:  # pragma: no cover - exercised on POSIX
    _msvcrt = None  # type: ignore[assignment]

_DEFAULT_LOG_DIR = Path.home() / ".sawa" / "logs"


def _positive_int_env(name: str, default: int) -> int:
    try:
        return max(1, int(os.environ.get(name, str(default))))
    except (TypeError, ValueError):
        return default


_FAILURE_ALERT_THRESHOLD = _positive_int_env("MCP_FAILURE_ALERT_THRESHOLD", 3)
_COUNTER_THREAD_LOCK = threading.RLock()
_ALERT_STORE_THREAD_LOCK = threading.RLock()
_ALERT_WORKER_LOCK = threading.Lock()
_ALERT_EVENT = threading.Event()
_ALERT_WORKER_STARTED = False
_ALERT_LOGGER = logging.getLogger(__name__)
_ALERT_LEASE_SECONDS = 60.0
_ALERT_RETRY_SECONDS = 5.0


class _PrivateTimedRotatingFileHandler(TimedRotatingFileHandler):
    """Timed handler whose newly-created base file is always mode 0600."""

    def _open(self) -> TextIOWrapper:
        return open_private_text(Path(self.baseFilename), self.mode)


def _state_file() -> Path:
    """Path to the cross-process failure-counter store.

    Resolved on each access so tests (and the audit log) can redirect it via
    ``MCP_LOG_DIR`` without re-importing the module.
    """
    log_dir = Path(os.environ.get("MCP_LOG_DIR") or _DEFAULT_LOG_DIR)
    return log_dir / "mcp_failure_counts.json"


def _pending_alerts_file() -> Path:
    """Durable alerts that have crossed the failure threshold."""
    return _state_file().with_name("mcp_pending_alerts.json")


def _load_counts() -> dict[str, int]:
    """Load persisted per-tool failure counts; empty dict on any problem."""
    try:
        with open_private_text(_state_file(), "r") as f:
            data = json.load(f)
    except (OSError, ValueError):
        return {}
    if not isinstance(data, dict):
        return {}
    # A valid persisted streak is non-negative through the alert threshold.
    # The threshold value is retained only when durable alert enqueue failed,
    # so the next failure retries instead of silently losing the alert. JSON's
    # decoder accepts NaN/Infinity by default, so reject non-finite numbers,
    # bools, fractions, and implausibly large corrupt values explicitly.
    counts: dict[str, int] = {}
    for key, value in data.items():
        if isinstance(value, bool):
            continue
        if isinstance(value, int):
            count = value
        elif isinstance(value, float) and math.isfinite(value) and value.is_integer():
            count = int(value)
        else:
            continue
        if 0 <= count <= _FAILURE_ALERT_THRESHOLD:
            counts[str(key)] = count
    return counts


@contextmanager
def _portable_file_lock(
    lock_path: Path,
    thread_lock: threading.RLock,
) -> Iterator[None]:
    """Serialize a private-file update across threads and supported OSes."""
    with thread_lock, open_private_text(lock_path, "a") as lock_file:
        if _fcntl is not None:
            _fcntl.flock(lock_file.fileno(), _fcntl.LOCK_EX)
            try:
                yield
            finally:
                _fcntl.flock(lock_file.fileno(), _fcntl.LOCK_UN)
            return

        if _msvcrt is not None:  # pragma: no cover - exercised on Windows
            # msvcrt.locking() locks bytes from the current position. Ensure
            # the private lock file contains one byte and always lock byte 0.
            lock_file.seek(0, os.SEEK_END)
            if lock_file.tell() == 0:
                lock_file.write("0")
                lock_file.flush()
            lock_file.seek(0)
            _msvcrt.locking(lock_file.fileno(), _msvcrt.LK_LOCK, 1)
            try:
                yield
            finally:
                lock_file.seek(0)
                _msvcrt.locking(lock_file.fileno(), _msvcrt.LK_UNLCK, 1)
            return

        # Python currently supports one of the locking APIs on every target
        # OS shipped by this project. Retain process-local safety on an
        # unexpected platform instead of making monitoring break tool calls.
        yield  # pragma: no cover


@contextmanager
def _counter_lock() -> Iterator[None]:
    """Serialize the read-modify-write cycle across MCP server processes."""
    with _portable_file_lock(
        _state_file().with_suffix(".lock"),
        _COUNTER_THREAD_LOCK,
    ):
        yield


@contextmanager
def _alert_store_lock() -> Iterator[None]:
    """Serialize durable alert queue updates across processes."""
    with _portable_file_lock(
        _pending_alerts_file().with_suffix(".lock"),
        _ALERT_STORE_THREAD_LOCK,
    ):
        yield


def _load_pending_alerts() -> dict[str, dict[str, Any]]:
    try:
        with open_private_text(_pending_alerts_file(), "r") as alert_file:
            data = json.load(alert_file)
    except (OSError, ValueError):
        return {}
    if not isinstance(data, dict):
        return {}

    pending: dict[str, dict[str, Any]] = {}
    for raw_tool, raw_record in data.items():
        if not isinstance(raw_record, dict):
            continue
        count = raw_record.get("count")
        generation = raw_record.get("generation")
        lease_until = raw_record.get("lease_until", 0.0)
        if (
            isinstance(count, bool)
            or not isinstance(count, int)
            or count < 1
            or isinstance(generation, bool)
            or not isinstance(generation, int)
            or generation < 1
            or isinstance(lease_until, bool)
            or not isinstance(lease_until, (int, float))
            or not math.isfinite(float(lease_until))
        ):
            continue
        tool = _truncate_utf8(str(raw_tool), 128)
        pending[tool] = {
            "tool": tool,
            "count": count,
            "error_type": _truncate_utf8(
                redact_sensitive_text(raw_record.get("error_type", "Unknown")),
                128,
            ),
            "error": _truncate_utf8(
                redact_sensitive_text(raw_record.get("error", "Unknown")),
                2048,
            ),
            "generation": generation,
            "lease_id": _truncate_utf8(str(raw_record.get("lease_id", "")), 64),
            "lease_until": float(lease_until),
        }
    return pending


def _atomic_save_json(path: Path, payload: object) -> bool:
    """Durably replace a private JSON file without exposing partial content."""
    temporary = path.with_name(f".{path.name}.{os.getpid()}.{uuid.uuid4().hex}.tmp")
    try:
        with open_private_text(temporary, "w") as output:
            json.dump(payload, output)
            output.flush()
            os.fsync(output.fileno())
        os.replace(temporary, path)
        ensure_private_file(path)
        return True
    except (OSError, TypeError, ValueError):
        # Monitoring is best-effort and must not change protocol results.
        return False
    finally:
        try:
            temporary.unlink(missing_ok=True)
        except OSError:
            pass


def _save_pending_alerts(pending: dict[str, dict[str, Any]]) -> bool:
    return _atomic_save_json(_pending_alerts_file(), pending)


def _queue_failure_alert(
    *,
    tool: str,
    count: int,
    err_type: str,
    safe_error: str,
) -> bool:
    """Persist one alert per tool, coalescing newer threshold crossings."""
    with _alert_store_lock():
        pending = _load_pending_alerts()
        previous = pending.get(tool, {})
        generation = int(previous.get("generation", 0)) + 1
        pending[tool] = {
            "tool": tool,
            "count": count,
            "error_type": err_type,
            "error": safe_error,
            "generation": generation,
            # Preserve an in-flight lease. Generation matching prevents its
            # completion from discarding this newly coalesced occurrence.
            "lease_id": previous.get("lease_id", ""),
            "lease_until": previous.get("lease_until", 0.0),
        }
        return _save_pending_alerts(pending)


def _claim_pending_alert() -> dict[str, Any] | None:
    """Lease one durable alert without holding a lock during network I/O."""
    now = time.time()
    with _alert_store_lock():
        pending = _load_pending_alerts()
        for tool in sorted(pending):
            record = pending[tool]
            if record["lease_id"] and record["lease_until"] > now:
                continue
            claimed = dict(record)
            claimed["lease_id"] = uuid.uuid4().hex
            claimed["lease_until"] = now + _ALERT_LEASE_SECONDS
            pending[tool] = claimed
            if _save_pending_alerts(pending):
                return dict(claimed)
            return None
    return None


def _complete_pending_alert(claimed: dict[str, Any], delivered: bool) -> None:
    """Acknowledge delivery, retaining newer or failed durable alerts."""
    tool = str(claimed["tool"])
    with _alert_store_lock():
        pending = _load_pending_alerts()
        current = pending.get(tool)
        if current is None or current["lease_id"] != claimed["lease_id"]:
            return
        if delivered and current["generation"] == claimed["generation"]:
            pending.pop(tool, None)
        else:
            current["lease_id"] = ""
            current["lease_until"] = 0.0
        _save_pending_alerts(pending)


def _send_failure_alert(
    logger: logging.Logger,
    *,
    tool: str,
    count: int,
    err_type: str,
    safe_error: str,
) -> bool:
    """Deliver one failure alert without delaying an MCP protocol response."""
    try:
        return bool(
            get_notifier(logger).send(
            title=f"Sawa MCP: {tool} failing",
            body=(
                f"Tool '{tool}' has failed {count} consecutive times.\n"
                f"Latest error: {err_type}: {safe_error}\n\n"
                "Counter will reset after the next success or after this alert."
            ),
            level=NotificationLevel.WARNING,
            tags=["warning", "mcp", tool],
            )
        )
    except Exception as exc:  # noqa: BLE001 - alerting is best-effort
        logger.warning(
            "MCP failure alert delivery failed: %s: %s",
            type(exc).__name__,
            _truncate_utf8(redact_sensitive_text(exc), 512),
        )
        return False


def _deliver_available_alerts(logger: logging.Logger) -> None:
    """Run one guarded delivery cycle so a transient error cannot kill the worker."""
    try:
        while claimed := _claim_pending_alert():
            delivered = _send_failure_alert(
                logger,
                tool=claimed["tool"],
                count=claimed["count"],
                err_type=claimed["error_type"],
                safe_error=claimed["error"],
            )
            _complete_pending_alert(claimed, delivered)
            if not delivered:
                break
    except Exception as exc:  # noqa: BLE001 - monitoring stays best-effort
        logger.warning(
            "MCP durable alert worker failed: %s: %s",
            type(exc).__name__,
            _truncate_utf8(redact_sensitive_text(exc), 512),
        )


def _alert_worker() -> None:
    """Serially deliver durable alerts; at most one worker exists per process."""
    while True:
        _ALERT_EVENT.wait(timeout=_ALERT_RETRY_SECONDS)
        _ALERT_EVENT.clear()
        _deliver_available_alerts(_ALERT_LOGGER)


def _wake_alert_worker(logger: logging.Logger) -> None:
    """Wake the single local worker; durable state survives process exit."""
    global _ALERT_LOGGER, _ALERT_WORKER_STARTED
    with _ALERT_WORKER_LOCK:
        _ALERT_LOGGER = logger
        if not _ALERT_WORKER_STARTED:
            threading.Thread(
                target=_alert_worker,
                name="sawa-mcp-alert-worker",
                daemon=True,
            ).start()
            _ALERT_WORKER_STARTED = True
    _ALERT_EVENT.set()


def _save_counts(counts: dict[str, int]) -> bool:
    """Persist failure counts; best-effort so monitoring never breaks a call."""
    return _atomic_save_json(_state_file(), counts)


def configure_file_logging(logger: logging.Logger) -> Path | None:
    """Attach a rotating file handler to the MCP root logger.

    Returns the resolved log file path so the caller can mention it on
    startup. ``None`` if the directory could not be created.
    """
    log_dir = Path(os.environ.get("MCP_LOG_DIR") or _DEFAULT_LOG_DIR)
    log_file = log_dir / "mcp.log"
    handler: logging.Handler | None = None
    try:
        ensure_private_directory(log_dir)
        ensure_private_file(log_file)
        handler = _PrivateTimedRotatingFileHandler(
            log_file,
            when="midnight",
            backupCount=14,
            encoding="utf-8",
        )
        handler.setLevel(logging.DEBUG)
        handler.addFilter(RedactingFilter())
        handler.setFormatter(
            RedactingFormatter(
                "%(asctime)s [%(levelname)s] %(name)s: %(message)s",
                datefmt="%Y-%m-%d %H:%M:%S",
            )
        )
        # Attach to the root logger so child loggers (mcp_server.*, sawa.*)
        # all flow into the same file without each having to opt in.
        logging.getLogger().addHandler(handler)
    except Exception as exc:  # noqa: BLE001 - logging must stay best-effort
        if handler is not None:
            handler.close()
        logger.warning("Could not configure MCP file log %s: %s", log_file, exc)
        return None
    return log_file


def _truncate_utf8(text: str, max_bytes: int) -> str:
    """Bound monitoring fields without splitting a UTF-8 code point."""
    encoded = text.encode("utf-8")
    if len(encoded) <= max_bytes:
        return text
    suffix = "…"
    suffix_bytes = len(suffix.encode("utf-8"))
    low, high = 0, len(text)
    while low < high:
        middle = (low + high + 1) // 2
        if len(text[:middle].encode("utf-8")) + suffix_bytes <= max_bytes:
            low = middle
        else:
            high = middle - 1
    return text[:low] + suffix


def record_call_outcome(
    tool: str,
    *,
    success: bool,
    duration_ms: float,
    logger: logging.Logger,
    error: BaseException | None = None,
    error_type: str | None = None,
) -> None:
    """Log a structured outcome line and, on failure, maybe fire an alert.

    Called from ``call_tool`` at the success and failure boundaries.
    """
    tool = _truncate_utf8(str(tool), 128)
    err_type = _truncate_utf8(
        redact_sensitive_text(
            error_type or (type(error).__name__ if error else "Unknown")
        ),
        128,
    )
    safe_error = _truncate_utf8(
        redact_sensitive_text(error) if error else "Unknown",
        2048,
    )
    enqueue_failed = False
    with _counter_lock():
        counts = _load_counts()

        if success:
            previous = counts.pop(tool, 0)
            if previous:
                _save_counts(counts)
            count = 0
            should_alert = False
        else:
            count = counts.get(tool, 0) + 1
            threshold_reached = count >= _FAILURE_ALERT_THRESHOLD
            should_alert = False
            if threshold_reached:
                should_alert = _queue_failure_alert(
                    tool=tool,
                    count=count,
                    err_type=err_type,
                    safe_error=safe_error,
                )
                enqueue_failed = not should_alert
            counts[tool] = (
                0
                if should_alert
                else min(count, _FAILURE_ALERT_THRESHOLD)
            )
            _save_counts(counts)

    if success:
        logger.info("[mcp] tool=%s duration_ms=%.1f status=ok", tool, duration_ms)
        if previous:
            logger.info("[mcp] tool=%s recovered after %d failure(s)", tool, previous)
        _wake_alert_worker(logger)
        return

    logger.warning(
        "[mcp] tool=%s duration_ms=%.1f status=error error_type=%s "
        "consecutive_failures=%d error=%s",
        tool,
        duration_ms,
        err_type,
        count,
        safe_error,
    )

    if enqueue_failed:
        logger.warning(
            "[mcp] durable failure alert enqueue failed; streak retained for retry"
        )
    _wake_alert_worker(logger)


def reset_counters() -> None:
    """Clear all persisted failure counters (test helper)."""
    try:
        with _counter_lock():
            _state_file().unlink(missing_ok=True)
    except OSError:
        pass
    try:
        with _alert_store_lock():
            _pending_alerts_file().unlink(missing_ok=True)
    except OSError:
        pass
