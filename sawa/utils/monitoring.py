"""Run-level monitoring: start/finish/failure banners + notifier dispatch.

``monitored_run`` is a context manager applied at CLI boundaries so every
data-producing action gets the same treatment regardless of how it is
invoked. It does not replace per-run logging inside the ``run_*`` functions
— those banners stay in the file log — it adds:

* an elapsed-time measurement,
* a single success notification (with the stats dict),
* a single failure notification on any uncaught exception,
* a consistent log line emitted at start/end for log-review tooling.

Success notifications can be disabled by setting ``SAWA_NOTIFY_SUCCESS=0`` —
useful when an outer scheduler already sends its own completion message.
"""

from __future__ import annotations

import logging
import os
import time
from collections.abc import Iterator
from contextlib import contextmanager
from typing import Any

from sawa.utils.notify import (
    MAX_NOTIFICATION_BODY_BYTES,
    MAX_NOTIFICATION_TITLE_BYTES,
    NotificationLevel,
    Notifier,
    get_notifier,
    sanitize_notification_text,
)
from sawa.utils.security import redact_sensitive_text


def _success_enabled() -> bool:
    val = os.environ.get("SAWA_NOTIFY_SUCCESS", "1").strip().lower()
    return val not in {"0", "false", "no", "off"}


def _format_stats(stats: dict[str, Any]) -> str:
    """Human-readable rendering of a stats dict for the notification body."""
    if not stats:
        return "(no stats)"
    lines: list[str] = []
    for key, value in stats.items():
        if key == "success":
            continue
        if isinstance(value, dict):
            inner = ", ".join(f"{k}={v}" for k, v in value.items())
            lines.append(f"{key}: {inner}")
        elif isinstance(value, list):
            lines.append(f"{key}: {len(value)} items")
        else:
            lines.append(f"{key}: {value}")
    return "\n".join(lines) if lines else "(no stats)"


@contextmanager
def monitored_run(
    name: str,
    *,
    logger: logging.Logger,
    notifier: Notifier | None = None,
    send_success: bool | None = None,
) -> Iterator[dict[str, Any]]:
    """Wrap a data-producing action with timing + success/failure alerts.

    Usage::

        with monitored_run("daily", logger=logger) as ctx:
            ctx["stats"] = run_daily(...)

    The yielded dict lets the caller stash the run's stats so the success
    notification can summarize them. On exception, the body includes whatever
    stats had been recorded before the failure.

    Args:
        name: Short job name (e.g. ``daily``, ``weekly``). Used in titles,
            tags, and log lines.
        logger: Logger used for the surrounding INFO/ERROR lines.
        notifier: Optional notifier instance; defaults to ``get_notifier()``.
        send_success: Whether to emit a notification on success. ``None``
            (default) honors the ``SAWA_NOTIFY_SUCCESS`` env var.
    """
    ctx: dict[str, Any] = {"stats": {}}
    notif = notifier or get_notifier(logger)
    notify_success = send_success if send_success is not None else _success_enabled()

    start = time.monotonic()
    logger.info("[%s] starting", name)

    try:
        yield ctx
    except BaseException as exc:
        elapsed = time.monotonic() - start
        safe_error = f"{type(exc).__name__}: {redact_sensitive_text(exc)}"
        # Do not attach the original traceback: logging formats exception text
        # after filters run, which could reintroduce a credential from str(exc).
        logger.error("[%s] failed after %.1fs: %s", name, elapsed, safe_error)
        body_parts = [
            safe_error,
            "",
            f"Ran {elapsed:.1f}s before failure.",
        ]
        partial = _format_stats(ctx.get("stats") or {})
        if partial and partial != "(no stats)":
            body_parts.extend(["", "Partial stats:", partial])
        notif.send(
            title=sanitize_notification_text(
                f"Sawa: {name} FAILED",
                MAX_NOTIFICATION_TITLE_BYTES,
                allow_newlines=False,
            ),
            body=sanitize_notification_text(
                "\n".join(body_parts), MAX_NOTIFICATION_BODY_BYTES
            ),
            level=NotificationLevel.ERROR,
            tags=["rotating_light", name],
        )
        raise

    elapsed = time.monotonic() - start
    stats = ctx.get("stats") or {}
    if stats.get("success") is False:
        logger.error("[%s] returned unsuccessful status after %.1fs", name, elapsed)
        notif.send(
            title=sanitize_notification_text(
                f"Sawa: {name} FAILED",
                MAX_NOTIFICATION_TITLE_BYTES,
                allow_newlines=False,
            ),
            body=sanitize_notification_text(
                f"Run returned an unsuccessful status after {elapsed:.1f}s.\n\n"
                f"{_format_stats(stats)}",
                MAX_NOTIFICATION_BODY_BYTES,
            ),
            level=NotificationLevel.ERROR,
            tags=["rotating_light", name],
        )
        return

    logger.info("[%s] complete in %.1fs", name, elapsed)
    if notify_success:
        notif.send(
            title=sanitize_notification_text(
                f"Sawa: {name} complete",
                MAX_NOTIFICATION_TITLE_BYTES,
                allow_newlines=False,
            ),
            body=sanitize_notification_text(
                f"Ran {elapsed:.1f}s.\n\n{_format_stats(ctx.get('stats') or {})}",
                MAX_NOTIFICATION_BODY_BYTES,
            ),
            level=NotificationLevel.INFO,
            tags=["white_check_mark", name],
        )
