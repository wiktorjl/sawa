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
