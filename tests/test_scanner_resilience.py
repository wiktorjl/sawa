"""Scanner provider-outage visibility regressions."""

import logging
from unittest.mock import AsyncMock, patch

import pytest

from sawa.api.async_client import AggregateBatchResult
from sawa.domain.exceptions import ProviderError
from sawa.scanner import scan_ytd_performance


@pytest.mark.asyncio
async def test_total_price_batch_failure_raises_provider_error() -> None:
    failed = AggregateBatchResult(
        failures={
            "AAPL": "ProviderError: unavailable",
            "MSFT": "ProviderError: unavailable",
        }
    )

    with patch("sawa.scanner.fetch_index_symbols", return_value=["AAPL", "MSFT"]), patch(
        "sawa.scanner.AsyncPolygonClient"
    ) as client_class:
        client_class.return_value.get_aggregates_batch = AsyncMock(return_value=failed)
        with pytest.raises(ProviderError, match="All 2 price requests failed"):
            await scan_ytd_performance(
                api_key="test-key",
                logger=logging.getLogger(__name__),
            )

    client_class.return_value.get_ticker_details.assert_not_called()


@pytest.mark.asyncio
async def test_empty_constituent_source_raises_before_provider_calls() -> None:
    with patch("sawa.scanner.fetch_index_symbols", return_value=[]), patch(
        "sawa.scanner.AsyncPolygonClient"
    ) as client_class:
        with pytest.raises(ProviderError, match="returned no symbols"):
            await scan_ytd_performance(
                api_key="test-key",
                logger=logging.getLogger(__name__),
            )

    client_class.assert_not_called()


@pytest.mark.asyncio
async def test_total_company_details_outage_raises_provider_error() -> None:
    prices = AggregateBatchResult(
        {
            "AAPL": [{"t": 1, "c": 100}, {"t": 2, "c": 101}],
            "MSFT": [{"t": 1, "c": 200}, {"t": 2, "c": 202}],
        }
    )

    with patch("sawa.scanner.fetch_index_symbols", return_value=["AAPL", "MSFT"]), patch(
        "sawa.scanner.AsyncPolygonClient"
    ) as client_class:
        client = client_class.return_value
        client.get_aggregates_batch = AsyncMock(return_value=prices)
        client.get_ticker_details = AsyncMock(side_effect=TimeoutError("outage"))

        with pytest.raises(ProviderError, match="All 2 company detail requests failed"):
            await scan_ytd_performance(
                api_key="test-key",
                logger=logging.getLogger(__name__),
            )
