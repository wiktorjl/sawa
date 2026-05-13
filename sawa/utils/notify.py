"""NTFY notifications and API-key alerting."""

import logging
import os

import httpx


def notify_ntfy(
    title: str,
    body: str,
    *,
    tags: str = "warning",
    logger: logging.Logger | None = None,
    timeout: float = 10.0,
) -> bool:
    """
    POST a notification to the NTFY topic configured in NTFY_TOPIC env var.

    Args:
        title: Notification title (sent as the ``Title`` header).
        body: Notification body (request payload).
        tags: Comma-separated ntfy.sh tag list (e.g., "warning,key").
        logger: Logger used for failure messages.
        timeout: HTTP timeout in seconds.

    Returns:
        True if the POST succeeded; False if NTFY_TOPIC is unset or the
        request failed. Failures are logged at WARNING but never raised.
    """
    log = logger or logging.getLogger(__name__)
    topic = os.environ.get("NTFY_TOPIC")
    if not topic:
        log.warning("NTFY_TOPIC not set; skipping notification: %s", title)
        return False

    url = topic if topic.startswith(("http://", "https://")) else f"https://{topic}"
    try:
        response = httpx.post(
            url,
            content=body.encode("utf-8"),
            headers={"Title": title, "Tags": tags},
            timeout=timeout,
        )
        response.raise_for_status()
        return True
    except Exception as exc:
        log.warning("NTFY notification failed (%s): %s", title, exc)
        return False


def alert_missing_api_key(
    env_var: str,
    purpose: str,
    logger: logging.Logger,
) -> None:
    """
    Log an ERROR and send an NTFY alert when a required API key is missing.

    Callers should invoke this at the skip point (where data would otherwise
    be fetched) so the operator gets a single, actionable alert per run.

    Args:
        env_var: Environment variable name (e.g., "FRED_API_KEY").
        purpose: What data won't be fetched (e.g., "FRED market internals
            (VIX, VIX3M, HY spread)").
        logger: Logger to write the ERROR line to.
    """
    msg = f"{env_var} not set - skipping {purpose}"
    logger.error(msg)
    notify_ntfy(
        f"Sawa: missing {env_var}",
        f"{msg}.\nSet the env var and re-run to backfill.",
        tags="warning,key",
        logger=logger,
    )
