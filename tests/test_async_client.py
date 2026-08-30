"""Async Polygon client batch visibility and retry-boundary tests."""

import asyncio
import logging
from datetime import date
from unittest.mock import AsyncMock, MagicMock, patch

import httpx
import pytest

from sawa.api.async_client import AggregateBatchResult, AsyncPolygonClient
from sawa.domain.exceptions import ProviderError


@pytest.mark.asyncio
async def test_batch_preserves_failed_ticker_and_redacts_error() -> None:
    secret = "batch-secret"
    request = httpx.Request(
        "GET", f"https://api.polygon.io/v2/aggs?apiKey={secret}"
    )
    provider_error = ProviderError(
        "request failed",
        provider="polygon",
        original_error=httpx.ReadTimeout("timed out", request=request),
    )
    client = AsyncPolygonClient("key", logging.getLogger(__name__))

    async def fetch_one(ticker: str, *_args: object, **_kwargs: object):
        if ticker == "MSFT":
            raise provider_error
        return [{"t": 1, "c": 2}]

    with patch.object(client, "get_aggregates", side_effect=fetch_one):
        result = await client.get_aggregates_batch(
            ["AAPL", "MSFT"], date(2026, 8, 1), date(2026, 8, 2)
        )

    assert isinstance(result, AggregateBatchResult)
    assert result == {"AAPL": [{"t": 1, "c": 2}]}
    assert set(result.failures) == {"MSFT"}
    assert secret not in result.failures["MSFT"]
    assert "ProviderError" in result.failures["MSFT"]


@pytest.mark.asyncio
async def test_batch_rejects_zero_concurrency() -> None:
    client = AsyncPolygonClient("key", logging.getLogger(__name__))

    with pytest.raises(ValueError, match="concurrency must be at least 1"):
        await client.get_aggregates_batch(
            ["AAPL"],
            date(2026, 8, 1),
            date(2026, 8, 2),
            concurrency=0,
        )


@pytest.mark.asyncio
async def test_batch_propagates_cancellation() -> None:
    client = AsyncPolygonClient("key", logging.getLogger(__name__))

    with patch.object(
        client,
        "get_aggregates",
        side_effect=asyncio.CancelledError(),
    ), pytest.raises(asyncio.CancelledError):
        await client.get_aggregates_batch(
            ["AAPL"], date(2026, 8, 1), date(2026, 8, 2)
        )


@pytest.mark.asyncio
async def test_terminal_rate_limit_does_not_sleep_after_last_attempt() -> None:
    response = MagicMock(spec=httpx.Response)
    response.status_code = 429
    http_client = MagicMock()
    http_client.get = AsyncMock(return_value=response)
    context = MagicMock()
    context.__aenter__ = AsyncMock(return_value=http_client)
    context.__aexit__ = AsyncMock(return_value=None)
    limiter = MagicMock()
    limiter.acquire = AsyncMock(return_value=None)
    client = AsyncPolygonClient(
        "key", logging.getLogger(__name__), rate_limiter=limiter
    )

    with patch(
        "sawa.api.async_client.httpx.AsyncClient", return_value=context
    ), patch("sawa.api.async_client.asyncio.sleep", new_callable=AsyncMock) as sleep:
        with pytest.raises(ProviderError, match="after 3 attempts"):
            await client.get_aggregates(
                "AAPL", date(2026, 8, 1), date(2026, 8, 2)
            )

    assert http_client.get.await_count == 3
    assert [call.args[0] for call in sleep.await_args_list] == [2, 4]


@pytest.mark.asyncio
async def test_http_200_provider_error_payload_is_not_treated_as_empty_data() -> None:
    response = MagicMock(spec=httpx.Response)
    response.status_code = 200
    response.raise_for_status.return_value = None
    response.json.return_value = {
        "status": "ERROR",
        "error": "invalid API key",
    }
    http_client = MagicMock()
    http_client.get = AsyncMock(return_value=response)
    context = MagicMock()
    context.__aenter__ = AsyncMock(return_value=http_client)
    context.__aexit__ = AsyncMock(return_value=None)
    limiter = MagicMock()
    limiter.acquire = AsyncMock(return_value=None)
    client = AsyncPolygonClient(
        "key", logging.getLogger(__name__), rate_limiter=limiter
    )

    with patch(
        "sawa.api.async_client.httpx.AsyncClient", return_value=context
    ), patch("sawa.api.async_client.asyncio.sleep", new_callable=AsyncMock) as sleep:
        with pytest.raises(ProviderError, match="invalid API key"):
            await client.get_aggregates(
                "AAPL", date(2026, 8, 1), date(2026, 8, 2)
            )

    assert http_client.get.await_count == 1
    sleep.assert_not_awaited()


@pytest.mark.asyncio
async def test_malformed_bar_isolated_from_good_batch_peer() -> None:
    client = AsyncPolygonClient("key", logging.getLogger(__name__))

    async def fetch_one(ticker: str, *_args: object, **_kwargs: object):
        if ticker == "BROKEN":
            raise ProviderError("Invalid response", provider="polygon")
        return [{"t": 1, "o": 1, "h": 2, "l": 1, "c": 2, "v": 10}]

    with patch.object(client, "get_aggregates", side_effect=fetch_one):
        result = await client.get_aggregates_batch(
            ["good", "broken"], date(2026, 8, 1), date(2026, 8, 2)
        )

    assert set(result) == {"GOOD"}
    assert set(result.failures) == {"BROKEN"}
    assert result.to_payload() == {
        "data": {
            "GOOD": [{"t": 1, "o": 1, "h": 2, "l": 1, "c": 2, "v": 10}]
        },
        "failures": {
            "BROKEN": "ProviderError: Invalid response (provider: polygon)"
        },
    }


@pytest.mark.asyncio
async def test_batch_snapshots_and_deduplicates_tickers() -> None:
    client = AsyncPolygonClient("key", logging.getLogger(__name__))
    calls: list[str] = []

    async def fetch_one(ticker: str, *_args: object, **_kwargs: object):
        calls.append(ticker)
        return []

    requested = ["aapl", "AAPL", " msft "]
    with patch.object(client, "get_aggregates", side_effect=fetch_one):
        result = await client.get_aggregates_batch(
            requested, date(2026, 8, 1), date(2026, 8, 2)
        )

    requested.append("LATE")
    assert calls == ["AAPL", "MSFT"]
    assert result == {"AAPL": [], "MSFT": []}
