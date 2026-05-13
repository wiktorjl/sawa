import logging
from typing import Any

import httpx
import pytest

from sawa.utils.notify import alert_missing_api_key, notify_ntfy


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

    def fake_post(url: str, *, content: bytes, headers: dict[str, str], timeout: float) -> httpx.Response:
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

    def fake_post(url: str, *, content: bytes, headers: dict[str, str], timeout: float) -> httpx.Response:
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
