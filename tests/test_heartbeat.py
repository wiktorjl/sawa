"""Watchdog heartbeat pings must be HTTPS-only, redirect-free and never raise."""

from __future__ import annotations

import logging

import httpx
import pytest

from sawa.utils.heartbeat import heartbeat_request_url, ping_heartbeat


def _client(handler) -> httpx.Client:
    return httpx.Client(transport=httpx.MockTransport(handler), follow_redirects=False)


def test_fail_suffix_is_appended_only_on_failure() -> None:
    url = "https://hc.example/ping/uuid"
    assert heartbeat_request_url(url) == url
    assert heartbeat_request_url(url, failed=True) == url + "/fail"
    assert heartbeat_request_url(url + "/", failed=True) == url + "/fail"


@pytest.mark.parametrize(
    "url",
    ["http://hc.example/ping", "https://user:pw@hc.example/ping", "ftp://x", "https://"],
)
def test_non_https_or_userinfo_urls_are_rejected(url: str) -> None:
    with pytest.raises(ValueError):
        heartbeat_request_url(url)


def test_ping_returns_true_on_2xx_and_sends_fail_variant() -> None:
    seen: list[str] = []

    def handler(request: httpx.Request) -> httpx.Response:
        seen.append(str(request.url))
        return httpx.Response(200, text="OK")

    with _client(handler) as client:
        assert ping_heartbeat("https://hc.example/ping", client=client) is True
        assert ping_heartbeat("https://hc.example/ping", failed=True, client=client) is True
    assert seen == ["https://hc.example/ping", "https://hc.example/ping/fail"]


def test_ping_does_not_follow_redirects_and_reports_false() -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(302, headers={"location": "https://elsewhere.example/"})

    with _client(handler) as client:
        assert ping_heartbeat("https://hc.example/ping", client=client) is False


def test_ping_swallows_transport_errors_without_logging_the_url(caplog) -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        raise httpx.ConnectError("boom https://hc.example/ping/secret")

    with caplog.at_level(logging.WARNING), _client(handler) as client:
        assert ping_heartbeat("https://hc.example/ping/secret", client=client) is False
    assert "secret" not in caplog.text
    assert "ConnectError" in caplog.text


def test_unset_url_is_a_noop() -> None:
    assert ping_heartbeat(None) is False
    assert ping_heartbeat("") is False
