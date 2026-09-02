"""Tests for sawa.api.client retry behavior."""

from typing import Any
from unittest.mock import MagicMock, call, patch

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


def _single_ok(results: dict[str, Any]) -> MagicMock:
    return _ok_response({"status": "OK", "results": results})


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
        second_page_call = mock_get.call_args_list[-1]
        # The key is merged into the next_url itself; passing it as params=
        # would drop the cursor Polygon encodes in that URL.
        assert second_page_call.args[0] == httpx.URL(
            "https://api.polygon.io/p2?apiKey=test-key"
        )
        assert "params" not in second_page_call.kwargs

    @pytest.mark.parametrize(
        "next_url",
        [
            "https://attacker.example/steal",
            "https://api.polygon.io.attacker.example/steal",
            "http://api.polygon.io/insecure",
            "https://user@api.polygon.io/credentialed",
            "https://api.polygon.io:444/wrong-port",
        ],
    )
    def test_rejects_untrusted_next_url_before_authenticated_request(
        self, next_url: str
    ) -> None:
        client = PolygonClient("pagination-secret")
        first_page = _ok_response(
            {"status": "OK", "results": [{"id": "a"}], "next_url": next_url}
        )

        with patch.object(client.client, "get", return_value=first_page) as mock_get:
            with pytest.raises(ProviderError, match="outside the configured HTTPS API origin"):
                client.get_paginated("news")

        assert mock_get.call_count == 1

    def test_next_url_cursor_survives_api_key_injection(self) -> None:
        """The api key must merge into next_url, never replace its query.

        httpx replaces an existing query string when params= is supplied, so
        passing the key that way dropped Polygon's cursor and re-requested
        page 1 until the repeated-URL guard aborted the fetch.
        """
        client = PolygonClient("test-key")
        first_page = _ok_response(
            {
                "status": "OK",
                "results": [{"id": "a"}],
                "next_url": "https://api.polygon.io/v2/reference/news?cursor=abc123",
            }
        )
        second_page = _ok_response({"status": "OK", "results": [{"id": "b"}]})

        with patch.object(
            client.client, "get", side_effect=[first_page, second_page]
        ) as mock_get:
            results = client.get_paginated("news", {"limit": 1000})

        assert results == [{"id": "a"}, {"id": "b"}]
        second_url = mock_get.call_args_list[1].args[0]
        assert second_url.params["cursor"] == "abc123"
        assert second_url.params["apiKey"] == "test-key"

    def test_next_url_api_key_is_not_duplicated(self) -> None:
        """A next_url that already carries a key gets ours, exactly once."""
        client = PolygonClient("test-key")
        first_page = _ok_response(
            {
                "status": "OK",
                "results": [{"id": "a"}],
                "next_url": "https://api.polygon.io/v2/x?cursor=c1&apiKey=stale",
            }
        )
        second_page = _ok_response({"status": "OK", "results": [{"id": "b"}]})

        with patch.object(
            client.client, "get", side_effect=[first_page, second_page]
        ) as mock_get:
            client.get_paginated("news")

        second_url = mock_get.call_args_list[1].args[0]
        assert second_url.params.get_list("apiKey") == ["test-key"]
        assert second_url.params["cursor"] == "c1"

    def test_relative_next_url_stays_on_polygon_origin(self) -> None:
        client = PolygonClient("test-key")
        first_page = _ok_response(
            {"status": "OK", "results": [{"id": "a"}], "next_url": "/v2/page/2"}
        )
        second_page = _ok_response({"status": "OK", "results": [{"id": "b"}]})

        with patch.object(client.client, "get", side_effect=[first_page, second_page]) as mock_get:
            results = client.get_paginated("news")

        assert results == [{"id": "a"}, {"id": "b"}]
        assert mock_get.call_args_list[1].args[0] == httpx.URL(
            "https://api.polygon.io/v2/page/2?apiKey=test-key"
        )

    def test_rejects_absolute_untrusted_initial_endpoint_before_io(self) -> None:
        client = PolygonClient("test-key")

        with patch.object(client.client, "get") as mock_get:
            with pytest.raises(ProviderError, match="outside the configured HTTPS API origin"):
                client.get_paginated("https://attacker.example/first")

        mock_get.assert_not_called()

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
            data = client.get(
                "aggregates",
                path_params={"ticker": "AAPL", "start": "x", "end": "y"},
            )

        assert data == {"status": "OK", "results": []}
        assert mock_get.call_count == 2

    def test_retries_on_429_then_succeeds(self) -> None:
        client = PolygonClient("test-key")
        ok = _ok_response({"status": "OK", "results": []})

        with patch.object(client.client, "get") as mock_get, patch("time.sleep"):
            mock_get.side_effect = [_rate_limited(), ok]
            data = client.get(
                "aggregates",
                path_params={"ticker": "AAPL", "start": "x", "end": "y"},
            )

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


class TestGetSingleRetry:
    """get_single distinguishes not-found from transient/provider failures."""

    def test_404_alone_returns_none_without_retry_or_sleep(self) -> None:
        client = PolygonClient("test-key")
        not_found = MagicMock(spec=httpx.Response)
        not_found.status_code = 404

        with patch.object(client.client, "get", return_value=not_found) as mock_get, patch(
            "sawa.api.client.time.sleep"
        ) as sleep:
            result = client.get_single("ticker-details", max_retries=3)

        assert result is None
        assert mock_get.call_count == 1
        sleep.assert_not_called()

    def test_request_errors_retry_then_return_result_with_exact_sleeps(self) -> None:
        client = PolygonClient("test-key")
        timeout = httpx.ReadTimeout("timed out")
        result = {"ticker": "AAPL"}

        with patch.object(client.client, "get") as mock_get, patch(
            "sawa.api.client.time.sleep"
        ) as sleep:
            mock_get.side_effect = [timeout, timeout, _single_ok(result)]
            actual = client.get_single("ticker-details", max_retries=3)

        assert actual == result
        assert mock_get.call_count == 3
        assert sleep.call_args_list == [call(1), call(1)]

    def test_persistent_request_error_raises_redacted_provider_error_without_terminal_sleep(
        self, caplog: pytest.LogCaptureFixture
    ) -> None:
        secret = "request-secret-value"
        request = httpx.Request(
            "GET", f"https://api.polygon.io/resource?apiKey={secret}"
        )
        timeout = httpx.ReadTimeout("timed out", request=request)
        client = PolygonClient("test-key")

        with patch.object(client.client, "get", side_effect=timeout) as mock_get, patch(
            "sawa.api.client.time.sleep"
        ) as sleep:
            with pytest.raises(ProviderError, match="Request failed after 3 attempts") as exc_info:
                client.get_single("ticker-details", max_retries=3)

        assert exc_info.value.provider == "polygon"
        assert secret not in str(exc_info.value)
        assert secret not in caplog.text
        assert mock_get.call_count == 3
        assert sleep.call_args_list == [call(1), call(1)]

    def test_429_retries_then_returns_result_with_backoff(self) -> None:
        client = PolygonClient("test-key")
        result = {"ticker": "AAPL"}

        with patch.object(client.client, "get") as mock_get, patch(
            "sawa.api.client.time.sleep"
        ) as sleep:
            mock_get.side_effect = [_rate_limited(), _rate_limited(), _single_ok(result)]
            actual = client.get_single("ticker-details", max_retries=3)

        assert actual == result
        assert mock_get.call_count == 3
        assert sleep.call_args_list == [call(2), call(4)]

    def test_5xx_retries_then_returns_result_with_exact_sleeps(self) -> None:
        client = PolygonClient("test-key")
        result = {"ticker": "AAPL"}

        with patch.object(client.client, "get") as mock_get, patch(
            "sawa.api.client.time.sleep"
        ) as sleep:
            mock_get.side_effect = [_http_error(503), _http_error(502), _single_ok(result)]
            actual = client.get_single("ticker-details", max_retries=3)

        assert actual == result
        assert mock_get.call_count == 3
        assert sleep.call_args_list == [call(1), call(1)]

    def test_persistent_429_raises_provider_error_without_terminal_sleep(self) -> None:
        client = PolygonClient("test-key")

        with patch.object(client.client, "get", return_value=_rate_limited()) as mock_get, patch(
            "sawa.api.client.time.sleep"
        ) as sleep:
            with pytest.raises(ProviderError, match="Rate limited after 3 attempts") as exc_info:
                client.get_single("ticker-details", max_retries=3)

        assert exc_info.value.provider == "polygon"
        assert mock_get.call_count == 3
        assert sleep.call_args_list == [call(2), call(4)]

    def test_persistent_5xx_raises_redacted_provider_error_without_terminal_sleep(
        self, caplog: pytest.LogCaptureFixture
    ) -> None:
        secret = "fred-secret-value"
        request = httpx.Request(
            "GET", f"https://api.polygon.io/resource?apiKey={secret}"
        )
        response = httpx.Response(503, request=request)
        client = PolygonClient("test-key")

        with patch.object(client.client, "get", return_value=response) as mock_get, patch(
            "sawa.api.client.time.sleep"
        ) as sleep:
            with pytest.raises(ProviderError) as exc_info:
                client.get_single("ticker-details", max_retries=3)

        assert exc_info.value.provider == "polygon"
        assert secret not in str(exc_info.value)
        assert secret not in caplog.text
        assert mock_get.call_count == 3
        assert sleep.call_args_list == [call(1), call(1)]

    def test_non_404_4xx_fails_immediately_without_sleep(self) -> None:
        client = PolygonClient("test-key")

        with patch.object(client.client, "get", return_value=_http_error(403)) as mock_get, patch(
            "sawa.api.client.time.sleep"
        ) as sleep:
            with pytest.raises(ProviderError, match="HTTP 403 request failed"):
                client.get_single("ticker-details", max_retries=3)

        assert mock_get.call_count == 1
        sleep.assert_not_called()

    def test_rejects_invalid_retry_count_before_io(self) -> None:
        client = PolygonClient("test-key")

        with patch.object(client.client, "get") as mock_get:
            with pytest.raises(ValueError, match="max_retries must be at least 1"):
                client.get_single("ticker-details", max_retries=0)

        mock_get.assert_not_called()

    @pytest.mark.parametrize(
        ("json_side_effect", "json_value"),
        [
            (ValueError("invalid JSON"), None),
            (None, ["not", "an", "object"]),
        ],
    )
    def test_invalid_top_level_json_fails_immediately_without_sleep(
        self,
        json_side_effect: Exception | None,
        json_value: object,
    ) -> None:
        client = PolygonClient("test-key")
        response = MagicMock(spec=httpx.Response)
        response.status_code = 200
        response.raise_for_status.return_value = None
        if json_side_effect is not None:
            response.json.side_effect = json_side_effect
        else:
            response.json.return_value = json_value

        with patch.object(client.client, "get", return_value=response) as mock_get, patch(
            "sawa.api.client.time.sleep"
        ) as sleep:
            with pytest.raises(ProviderError):
                client.get_single("ticker-details", max_retries=3)

        assert mock_get.call_count == 1
        sleep.assert_not_called()

    @pytest.mark.parametrize(
        "payload",
        [
            {"status": "ERROR", "error": "provider rejected request"},
            {"status": "OK", "results": []},
        ],
    )
    def test_non_not_found_invalid_payloads_raise_provider_error(
        self, payload: dict[str, Any]
    ) -> None:
        client = PolygonClient("test-key")

        with patch.object(client.client, "get", return_value=_ok_response(payload)):
            with pytest.raises(ProviderError):
                client.get_single("ticker-details")
