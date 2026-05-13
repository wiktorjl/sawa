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
from contextlib import contextmanager
from typing import Any, Iterator

from sawa.utils.notify import (
    NotificationLevel,
    Notifier,
    get_notifier,
)


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
        logger.exception("[%s] failed after %.1fs", name, elapsed)
        body_parts = [
            f"{type(exc).__name__}: {exc}",
            "",
            f"Ran {elapsed:.1f}s before failure.",
        ]
        partial = _format_stats(ctx.get("stats") or {})
        if partial and partial != "(no stats)":
            body_parts.extend(["", "Partial stats:", partial])
        notif.send(
            title=f"Sawa: {name} FAILED",
            body="\n".join(body_parts),
            level=NotificationLevel.ERROR,
            tags=["rotating_light", name],
        )
        raise

    elapsed = time.monotonic() - start
    logger.info("[%s] complete in %.1fs", name, elapsed)
    if notify_success:
        notif.send(
            title=f"Sawa: {name} complete",
            body=f"Ran {elapsed:.1f}s.\n\n{_format_stats(ctx.get('stats') or {})}",
            level=NotificationLevel.INFO,
            tags=["white_check_mark", name],
        )
