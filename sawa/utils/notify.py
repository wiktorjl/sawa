"""Notification abstraction with pluggable backends (default: NTFY).

This module provides:

* ``NotificationLevel`` — semantic severity used by all callers.
* ``Notifier`` — Protocol every backend implements.
* ``NullNotifier`` — silent fallback when no backend is configured.
* ``NtfyNotifier`` — posts to an ntfy.sh-style HTTP topic.
* ``get_notifier()`` — factory that chooses a backend from environment.
* ``notify_ntfy()`` / ``alert_missing_api_key()`` — backward-compatible
  helpers preserved for existing call sites.

To swap backends in the future, implement ``Notifier`` and dispatch from
``get_notifier()`` based on the ``SAWA_NOTIFIER`` env var.
"""

from __future__ import annotations

import logging
import os
import time
from enum import Enum
from typing import Protocol

import httpx

# Delivery attempts for a single notification before giving up. A transient
# backend blip should not lose an alert; the dead-man's-switch heartbeat in the
# scheduler covers a fully unreachable backend.
_SEND_ATTEMPTS = 3
_SEND_RETRY_BACKOFF = 1.0


class NotificationLevel(str, Enum):
    """Severity used to map onto backend-specific priorities/colors."""

    INFO = "info"
    WARNING = "warning"
    ERROR = "error"


class Notifier(Protocol):
    """Send operator-visible notifications. Implementations must not raise."""

    def send(
        self,
        *,
        title: str,
        body: str,
        level: NotificationLevel = NotificationLevel.INFO,
        tags: list[str] | None = None,
    ) -> bool:
        ...


class NullNotifier:
    """Notifier that silently drops every message.

    Used when no backend is configured. ``send`` returns ``False`` so callers
    can detect that no delivery occurred.
    """

    def send(
        self,
        *,
        title: str,
        body: str,
        level: NotificationLevel = NotificationLevel.INFO,
        tags: list[str] | None = None,
    ) -> bool:
        return False


class NtfyNotifier:
    """POST notifications to an ntfy.sh-compatible HTTP endpoint.

    ``topic`` may be a bare topic name (``my-topic``), a host+topic pair
    (``ntfy.sh/my-topic``), or a full URL (``https://ntfy.example/topic``).
    """

    # Map NotificationLevel -> ntfy Priority header (1=min, 5=max).
    _PRIORITY = {
        NotificationLevel.INFO: "3",
        NotificationLevel.WARNING: "4",
        NotificationLevel.ERROR: "5",
    }

    def __init__(
        self,
        topic: str,
        *,
        timeout: float = 10.0,
        logger: logging.Logger | None = None,
    ) -> None:
        self.topic = topic
        self.timeout = timeout
        self.logger = logger or logging.getLogger(__name__)

    @property
    def url(self) -> str:
        if self.topic.startswith(("http://", "https://")):
            return self.topic
        return f"https://{self.topic}"

    def send(
        self,
        *,
        title: str,
        body: str,
        level: NotificationLevel = NotificationLevel.INFO,
        tags: list[str] | None = None,
    ) -> bool:
        headers = {
            "Title": title,
            "Priority": self._PRIORITY[level],
        }
        if tags:
            headers["Tags"] = ",".join(tags)

        last_exc: Exception | None = None
        for attempt in range(1, _SEND_ATTEMPTS + 1):
            try:
                response = httpx.post(
                    self.url,
                    content=body.encode("utf-8"),
                    headers=headers,
                    timeout=self.timeout,
                )
                response.raise_for_status()
                return True
            except Exception as exc:
                last_exc = exc
                if attempt < _SEND_ATTEMPTS:
                    time.sleep(_SEND_RETRY_BACKOFF * attempt)

        # All attempts failed. Losing an ERROR-level alert is itself serious, so
        # escalate the log level — the operator's last line of defence is the
        # scheduler's external heartbeat, not this swallowed failure.
        log = self.logger.error if level == NotificationLevel.ERROR else self.logger.warning
        log(
            "NTFY notification failed after %d attempts (%s): %s",
            _SEND_ATTEMPTS,
            title,
            last_exc,
        )
        return False


def get_notifier(logger: logging.Logger | None = None) -> Notifier:
    """Return the configured notifier based on environment.

    Resolution order:

    1. ``SAWA_NOTIFIER`` explicit choice (``ntfy``, ``none``). Unknown values
       fall through to autodetection with a warning.
    2. ``NTFY_TOPIC`` set → ``NtfyNotifier``.
    3. Otherwise → ``NullNotifier``.
    """
    log = logger or logging.getLogger(__name__)

    explicit = os.environ.get("SAWA_NOTIFIER", "").strip().lower()
    if explicit == "none":
        return NullNotifier()
    if explicit and explicit not in {"ntfy", "auto", ""}:
        log.warning("Unknown SAWA_NOTIFIER=%r, falling back to autodetect", explicit)

    topic = os.environ.get("NTFY_TOPIC")
    if topic:
        return NtfyNotifier(topic, logger=log)

    return NullNotifier()


def notify_ntfy(
    title: str,
    body: str,
    *,
    tags: str = "warning",
    logger: logging.Logger | None = None,
    timeout: float = 10.0,
) -> bool:
    """Backward-compatible wrapper that POSTs to ``NTFY_TOPIC``.

    Prefer ``get_notifier()`` for new code. Kept so existing call sites and
    tests continue to work unchanged.
    """
    log = logger or logging.getLogger(__name__)
    topic = os.environ.get("NTFY_TOPIC")
    if not topic:
        log.warning("NTFY_TOPIC not set; skipping notification: %s", title)
        return False

    notifier = NtfyNotifier(topic, timeout=timeout, logger=log)
    tag_list = [t for t in tags.split(",") if t] if tags else None
    return notifier.send(
        title=title,
        body=body,
        level=NotificationLevel.WARNING,
        tags=tag_list,
    )


def alert_missing_api_key(
    env_var: str,
    purpose: str,
    logger: logging.Logger,
    *,
    notifier: Notifier | None = None,
) -> None:
    """Log an ERROR and notify when a required API key is missing.

    Callers should invoke this at the skip point so the operator gets a single
    actionable alert per run.
    """
    msg = f"{env_var} not set - skipping {purpose}"
    logger.error(msg)
    (notifier or get_notifier(logger)).send(
        title=f"Sawa: missing {env_var}",
        body=f"{msg}.\nSet the env var and re-run to backfill.",
        level=NotificationLevel.WARNING,
        tags=["warning", "key"],
    )
