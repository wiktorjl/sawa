"""Tests for sawa.api.client retry behavior."""

from typing import Any
from unittest.mock import MagicMock, patch

import httpx
import pytest

from sawa.api.client import PolygonClient
from sawa.domain.exceptions import ProviderError


def _ok_response(payload: dict[str, Any]) -> MagicMock:
    resp = MagicMock(spec=httpx.Response)
    resp.status_code = 200
    resp.json.return_value = payload
    resp.raise_for_status.return_value = None
    return resp


def _rate_limited() -> MagicMock:
    resp = MagicMock(spec=httpx.Response)
    resp.status_code = 429
    return resp


def _http_error(status_code: int) -> MagicMock:
    """Response whose raise_for_status() raises HTTPStatusError (non-429)."""
    resp = MagicMock(spec=httpx.Response)
    resp.status_code = status_code
    resp.raise_for_status.side_effect = httpx.HTTPStatusError(
        f"{status_code}", request=MagicMock(), response=resp
    )
    return resp


class TestGetPaginatedRetry:
    """get_paginated must retry transient failures per page (mirrors get_single)."""

    def test_retries_then_succeeds_on_request_error(self) -> None:
        client = PolygonClient("test-key")
        ok = _ok_response({"status": "OK", "results": [{"id": "a"}]})

        with patch.object(client.client, "get") as mock_get, patch("time.sleep"):
            mock_get.side_effect = [
                httpx.ReadTimeout("timed out"),
                httpx.ReadTimeout("timed out"),
                ok,
            ]
            results = client.get_paginated("news", {"limit": 10})

        assert results == [{"id": "a"}]
        assert mock_get.call_count == 3

    def test_raises_after_exhausting_retries_on_request_error(self) -> None:
        client = PolygonClient("test-key")

        with patch.object(client.client, "get") as mock_get, patch("time.sleep"):
            mock_get.side_effect = httpx.ReadTimeout("timed out")
            with pytest.raises(httpx.ReadTimeout):
                client.get_paginated("news", {"limit": 10}, max_retries=2)

        assert mock_get.call_count == 2

    def test_retries_on_429_then_succeeds(self) -> None:
        client = PolygonClient("test-key")
        ok = _ok_response({"status": "OK", "results": [{"id": "b"}]})

        with patch.object(client.client, "get") as mock_get, patch("time.sleep"):
            mock_get.side_effect = [_rate_limited(), ok]
            results = client.get_paginated("news", {"limit": 10})

        assert results == [{"id": "b"}]
        assert mock_get.call_count == 2

    def test_raises_provider_error_on_persistent_429(self) -> None:
        client = PolygonClient("test-key")

        with patch.object(client.client, "get") as mock_get, patch("time.sleep"):
            mock_get.return_value = _rate_limited()
            with pytest.raises(ProviderError, match="Rate limited"):
                client.get_paginated("news", {"limit": 10}, max_retries=2)

        assert mock_get.call_count == 2

    def test_retry_resets_per_page(self) -> None:
        """A transient error on page 2 must not consume page 1's retry budget."""
        client = PolygonClient("test-key")
        page1 = _ok_response(
            {"status": "OK", "results": [{"id": "a"}], "next_url": "https://api.polygon.io/p2"}
        )
        page2 = _ok_response({"status": "OK", "results": [{"id": "b"}]})

        with patch.object(client.client, "get") as mock_get, patch("time.sleep"):
            mock_get.side_effect = [
                page1,
                httpx.ReadTimeout("timed out"),
                httpx.ReadTimeout("timed out"),
                page2,
            ]
            results = client.get_paginated("news", {"limit": 10})

        assert results == [{"id": "a"}, {"id": "b"}]
        assert mock_get.call_count == 4

    def test_http_status_error_becomes_provider_error(self) -> None:
        """A non-429 4xx/5xx must surface as ProviderError (callers catch that),
        not escape as an uncaught httpx.HTTPStatusError."""
        client = PolygonClient("test-key")

        with patch.object(client.client, "get") as mock_get, patch("time.sleep"):
            mock_get.return_value = _http_error(404)
            with pytest.raises(ProviderError):
                client.get_paginated("news", {"limit": 10})


class TestGetRetry:
    """get() (single-GET aggregates path) must retry transient failures."""

    def test_retries_on_request_error_then_succeeds(self) -> None:
        client = PolygonClient("test-key")
        ok = _ok_response({"status": "OK", "results": []})

        with patch.object(client.client, "get") as mock_get, patch("time.sleep"):
            mock_get.side_effect = [httpx.ReadTimeout("timed out"), ok]
            data = client.get("aggregates", path_params={"ticker": "AAPL", "start": "x", "end": "y"})

        assert data == {"status": "OK", "results": []}
        assert mock_get.call_count == 2

    def test_retries_on_429_then_succeeds(self) -> None:
        client = PolygonClient("test-key")
        ok = _ok_response({"status": "OK", "results": []})

        with patch.object(client.client, "get") as mock_get, patch("time.sleep"):
            mock_get.side_effect = [_rate_limited(), ok]
            data = client.get("aggregates", path_params={"ticker": "AAPL", "start": "x", "end": "y"})

        assert data == {"status": "OK", "results": []}
        assert mock_get.call_count == 2

    def test_retries_on_5xx_then_raises_provider_error(self) -> None:
        client = PolygonClient("test-key")

        with patch.object(client.client, "get") as mock_get, patch("time.sleep"):
            mock_get.return_value = _http_error(503)
            with pytest.raises(ProviderError):
                client.get(
                    "aggregates",
                    path_params={"ticker": "AAPL", "start": "x", "end": "y"},
                    max_retries=2,
                )

        assert mock_get.call_count == 2

    def test_4xx_raises_provider_error_without_retry(self) -> None:
        client = PolygonClient("test-key")

        with patch.object(client.client, "get") as mock_get, patch("time.sleep"):
            mock_get.return_value = _http_error(400)
            with pytest.raises(ProviderError):
                client.get(
                    "aggregates",
                    path_params={"ticker": "AAPL", "start": "x", "end": "y"},
                )

        # 4xx is not retried.
        assert mock_get.call_count == 1
