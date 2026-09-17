"""Dead-man's-switch pings for healthchecks.io-style monitors.

A heartbeat URL is a bearer capability: whoever holds it can silence the
monitor. It is therefore never logged, never placed in argv, and only ever
requested over HTTPS without following redirects.
"""

from __future__ import annotations

import logging
from urllib.parse import urlsplit, urlunsplit

import httpx

HEARTBEAT_TIMEOUT_SECONDS = 10.0
FAIL_SUFFIX = "/fail"


def heartbeat_request_url(url: str, *, failed: bool = False) -> str:
    """Validate a heartbeat URL and append ``/fail`` when the run failed."""
    parts = urlsplit(url)
    if parts.scheme != "https" or not parts.netloc:
        raise ValueError("heartbeat URL must use HTTPS")
    if parts.username is not None or parts.password is not None:
        raise ValueError("heartbeat URL must not contain userinfo")
    path = parts.path.rstrip("/") + FAIL_SUFFIX if failed else parts.path
    return urlunsplit((parts.scheme, parts.netloc, path, parts.query, ""))


def ping_heartbeat(
    url: str | None,
    *,
    failed: bool = False,
    logger: logging.Logger | None = None,
    client: httpx.Client | None = None,
) -> bool:
    """GET the heartbeat URL (or its ``/fail`` variant). Never raises.

    Returns True only when the monitor acknowledged the ping with a 2xx. An
    unset URL is a silent no-op so hosts without a monitor need no config.
    """
    log = logger or logging.getLogger(__name__)
    if not url:
        return False
    try:
        request_url = heartbeat_request_url(url, failed=failed)
    except ValueError as exc:
        log.warning("Heartbeat skipped: %s", exc)
        return False

    owns_client = client is None
    http = client or httpx.Client(timeout=HEARTBEAT_TIMEOUT_SECONDS, follow_redirects=False)
    try:
        response = http.get(request_url)
    except httpx.HTTPError as exc:
        # The exception text can embed the request URL; log only the class.
        log.warning("Heartbeat ping failed: %s", type(exc).__name__)
        return False
    finally:
        if owns_client:
            http.close()

    if 200 <= response.status_code < 300:
        return True
    log.warning("Heartbeat ping returned HTTP %s", response.status_code)
    return False
