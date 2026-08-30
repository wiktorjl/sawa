import logging
from typing import Any

import httpx
import pytest

from sawa.utils.notify import (
    MAX_NOTIFICATION_BODY_BYTES,
    MAX_NOTIFICATION_TITLE_BYTES,
    NotificationLevel,
    NtfyNotifier,
    NullNotifier,
    alert_missing_api_key,
    get_notifier,
    notify_ntfy,
)


class _CapHandler(logging.Handler):
    def __init__(self) -> None:
        super().__init__()
        self.records: list[logging.LogRecord] = []

    def emit(self, record: logging.LogRecord) -> None:
        self.records.append(record)


def _logger_with_capture() -> tuple[logging.Logger, _CapHandler]:
    log = logging.getLogger(f"test.notify.{id(object())}")
    log.handlers.clear()
    log.setLevel(logging.DEBUG)
    handler = _CapHandler()
    log.addHandler(handler)
    log.propagate = False
    return log, handler


def test_notify_ntfy_skips_when_topic_unset(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("NTFY_TOPIC", raising=False)
    log, cap = _logger_with_capture()

    sent = notify_ntfy("Title", "Body", logger=log)

    assert sent is False
    assert any("NTFY_TOPIC not set" in r.getMessage() for r in cap.records)


def test_notify_ntfy_posts_to_topic(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("NTFY_TOPIC", "ntfy.sh/MyTopic")
    calls: list[dict[str, Any]] = []

    def fake_post(
        url: str, *, content: bytes, headers: dict[str, str], timeout: float
    ) -> httpx.Response:
        calls.append({"url": url, "content": content, "headers": headers, "timeout": timeout})
        return httpx.Response(200, request=httpx.Request("POST", url))

    monkeypatch.setattr(httpx, "post", fake_post)
    log, _ = _logger_with_capture()

    sent = notify_ntfy("Sawa: missing FRED_API_KEY", "body text", tags="warning,key", logger=log)

    assert sent is True
    assert len(calls) == 1
    call = calls[0]
    assert call["url"] == "https://ntfy.sh/MyTopic"
    assert call["content"] == b"body text"
    assert call["headers"]["Title"] == "Sawa: missing FRED_API_KEY"
    assert call["headers"]["Tags"] == "warning,key"


def test_notify_ntfy_accepts_full_url(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("NTFY_TOPIC", "https://example.com/topic")
    captured: dict[str, Any] = {}

    def fake_post(url: str, **_: Any) -> httpx.Response:
        captured["url"] = url
        return httpx.Response(200, request=httpx.Request("POST", url))

    monkeypatch.setattr(httpx, "post", fake_post)

    assert notify_ntfy("t", "b") is True
    assert captured["url"] == "https://example.com/topic"


def test_notify_ntfy_swallows_http_errors(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("NTFY_TOPIC", "ntfy.sh/MyTopic")

    def fake_post(*_: Any, **__: Any) -> httpx.Response:
        raise httpx.ConnectError("boom")

    monkeypatch.setattr(httpx, "post", fake_post)
    log, cap = _logger_with_capture()

    sent = notify_ntfy("t", "b", logger=log)

    assert sent is False
    assert any("NTFY notification failed" in r.getMessage() for r in cap.records)


def test_alert_missing_api_key_logs_error_and_notifies(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("NTFY_TOPIC", "ntfy.sh/MyTopic")
    posts: list[dict[str, Any]] = []

    def fake_post(
        url: str, *, content: bytes, headers: dict[str, str], timeout: float
    ) -> httpx.Response:
        posts.append({"title": headers.get("Title"), "body": content.decode()})
        return httpx.Response(200, request=httpx.Request("POST", url))

    monkeypatch.setattr(httpx, "post", fake_post)
    log, cap = _logger_with_capture()

    alert_missing_api_key("FRED_API_KEY", "FRED market internals (VIX)", log)

    errors = [r for r in cap.records if r.levelno == logging.ERROR]
    assert errors, "expected an ERROR log line"
    assert "FRED_API_KEY" in errors[0].getMessage()
    assert "FRED market internals (VIX)" in errors[0].getMessage()

    assert len(posts) == 1
    assert posts[0]["title"] == "Sawa: missing FRED_API_KEY"
    assert "FRED_API_KEY" in posts[0]["body"]


def test_null_notifier_returns_false() -> None:
    n = NullNotifier()
    assert n.send(title="t", body="b") is False
    assert n.send(title="t", body="b", level=NotificationLevel.ERROR) is False


def test_ntfy_notifier_sets_priority_and_tags(monkeypatch: pytest.MonkeyPatch) -> None:
    captured: dict[str, Any] = {}

    def fake_post(
        url: str, *, content: bytes, headers: dict[str, str], timeout: float
    ) -> httpx.Response:
        captured.update(url=url, headers=headers, content=content)
        return httpx.Response(200, request=httpx.Request("POST", url))

    monkeypatch.setattr(httpx, "post", fake_post)
    n = NtfyNotifier("ntfy.sh/MyTopic")

    sent = n.send(
        title="Sawa: daily FAILED",
        body="something broke",
        level=NotificationLevel.ERROR,
        tags=["rotating_light", "daily"],
    )

    assert sent is True
    assert captured["url"] == "https://ntfy.sh/MyTopic"
    assert captured["headers"]["Title"] == "Sawa: daily FAILED"
    assert captured["headers"]["Priority"] == "5"
    assert captured["headers"]["Tags"] == "rotating_light,daily"
    assert captured["content"] == b"something broke"


def test_ntfy_notifier_swallows_errors(monkeypatch: pytest.MonkeyPatch) -> None:
    def fake_post(*_: Any, **__: Any) -> httpx.Response:
        raise httpx.ConnectError("net is down")

    monkeypatch.setattr(httpx, "post", fake_post)
    n = NtfyNotifier("ntfy.sh/T")
    assert n.send(title="t", body="b") is False


def test_ntfy_notifier_does_not_log_topic_on_http_status_error(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    secret_topic = "unguessable-publish-capability"

    def fake_post(url: str, **_: Any) -> httpx.Response:
        return httpx.Response(500, request=httpx.Request("POST", url))

    monkeypatch.setattr(httpx, "post", fake_post)
    log, cap = _logger_with_capture()
    notifier = NtfyNotifier(f"ntfy.sh/{secret_topic}", logger=log)

    assert notifier.send(title="failed", body="body") is False

    logged = "\n".join(record.getMessage() for record in cap.records)
    assert secret_topic not in logged
    assert "HTTP 500" in logged


def test_ntfy_notifier_redacts_and_bounds_outbound_payload(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    captured: dict[str, Any] = {}

    def fake_post(
        url: str, *, content: bytes, headers: dict[str, str], timeout: float
    ) -> httpx.Response:
        captured.update(content=content, headers=headers)
        return httpx.Response(200, request=httpx.Request("POST", url))

    monkeypatch.setattr(httpx, "post", fake_post)
    secret = "outbound-secret"
    notifier = NtfyNotifier("ntfy.sh/T")

    assert notifier.send(
        title=f"token={secret} " + "界" * 1_000,
        body=(
            f"postgresql://reader:{secret}@db.invalid/sawa "
            f"Bearer {secret} "
            + "界" * 10_000
        ),
    )

    title = captured["headers"]["Title"]
    body = captured["content"]
    assert isinstance(title, str)
    assert isinstance(body, bytes)
    assert secret not in title
    assert secret.encode() not in body
    assert "<redacted>" in title
    assert b"<redacted>" in body
    assert len(title.encode()) <= MAX_NOTIFICATION_TITLE_BYTES
    assert len(body) <= MAX_NOTIFICATION_BODY_BYTES


def test_ntfy_notifier_omits_secret_oversized_and_control_character_tags(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    captured: dict[str, Any] = {}

    def fake_post(url: str, **kwargs: Any) -> httpx.Response:
        captured.update(kwargs)
        return httpx.Response(200, request=httpx.Request("POST", url))

    monkeypatch.setattr(httpx, "post", fake_post)
    notifier = NtfyNotifier("ntfy.sh/T")

    assert notifier.send(
        title="safe\r\nInjected: header",
        body="body",
        tags=[
            "daily",
            "token=tag-secret",
            "x" * 1_000,
            "bad\r\ntag",
            "white_check_mark",
        ],
    )

    headers = captured["headers"]
    assert headers["Title"] == "safe Injected: header"
    assert headers["Tags"] == "daily,white_check_mark"
    assert "tag-secret" not in str(headers)


def test_get_notifier_returns_null_when_no_topic(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("NTFY_TOPIC", raising=False)
    monkeypatch.delenv("SAWA_NOTIFIER", raising=False)
    assert isinstance(get_notifier(), NullNotifier)


def test_get_notifier_returns_ntfy_when_topic_set(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("SAWA_NOTIFIER", raising=False)
    monkeypatch.setenv("NTFY_TOPIC", "ntfy.sh/MyTopic")
    n = get_notifier()
    assert isinstance(n, NtfyNotifier)
    assert n.url == "https://ntfy.sh/MyTopic"


def test_get_notifier_honors_explicit_none(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("NTFY_TOPIC", "ntfy.sh/MyTopic")
    monkeypatch.setenv("SAWA_NOTIFIER", "none")
    assert isinstance(get_notifier(), NullNotifier)
