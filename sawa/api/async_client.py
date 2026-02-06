"""
Async client for Polygon.io REST API.

This module provides an async HTTP client for interacting with Polygon.io API,
designed for concurrent requests and batch operations.

Usage:
    from sawa.api.async_client import AsyncPolygonClient

    client = AsyncPolygonClient(api_key="YOUR_KEY", logger=logger)
    prices = await client.get_aggregates("AAPL", start_date, end_date)
"""

import asyncio
import logging
from datetime import date
from typing import Any

import httpx

from sawa.domain.exceptions import ProviderError
from sawa.repositories.rate_limiter import SyncRateLimiter

DEFAULT_TIMEOUT = 30
MAX_REQUESTS_PER_MINUTE = 5


class AsyncPolygonClient:
    """Async client for Polygon.io REST API."""

    def __init__(
        self,
        api_key: str,
        logger: logging.Logger,
        rate_limiter: SyncRateLimiter | None = None,
        timeout: int = DEFAULT_TIMEOUT,
    ):
        """Initialize async client.

        Args:
            api_key: Polygon API key
            logger: Logger instance
            rate_limiter: Optional rate limiter
            timeout: Request timeout in seconds
        """
        self.api_key = api_key
        self.logger = logger
        self.base_url = "https://api.polygon.io"
        self.timeout = timeout
        self.rate_limiter = rate_limiter or SyncRateLimiter(
            requests_per_second=MAX_REQUESTS_PER_MINUTE / 60.0,
        )

    async def get_aggregates(
        self,
        ticker: str,
        start_date: date,
        end_date: date,
        timespan: str = "day",
        adjusted: bool = True,
        sort: str = "asc",
        limit: int = 50000,
    ) -> list[dict[str, Any]]:
        """
        Get aggregate bars (OHLCV) for a ticker.

        Args:
            ticker: Stock ticker symbol
            start_date: Start date
            end_date: End date
            timespan: Size of time window (day, hour, etc.)
            adjusted: Adjust for splits
            sort: Sort order (asc or desc)
            limit: Max number of results

        Returns:
            List of OHLCV records
        """
        url = (
            f"{self.base_url}/v2/aggs/ticker/{ticker.upper()}"
            f"/range/1/{timespan}/{start_date}/{end_date}"
        )
        params = {
            "adjusted": str(adjusted).lower(),
            "sort": sort,
            "limit": limit,
            "apiKey": self.api_key,
        }

        async with httpx.AsyncClient(timeout=self.timeout) as client:
            # Rate limiting (synchronous check before async call)
            self.rate_limiter.acquire()

            try:
                response = await client.get(url, params=params)
                response.raise_for_status()
                data = response.json()
                return data.get("results", [])
            except httpx.HTTPStatusError as e:
                self.logger.error(f"HTTP error for {ticker}: {e}")
                raise ProviderError(
                    f"HTTP error fetching aggregates for {ticker}: {e.response.status_code}",
                    provider="polygon",
                    original_error=e,
                ) from e
            except httpx.RequestError as e:
                self.logger.error(f"Request error for {ticker}: {e}")
                raise ProviderError(
                    f"Request error fetching aggregates for {ticker}",
                    provider="polygon",
                    original_error=e,
                ) from e

    async def get_ticker_details(self, ticker: str) -> dict[str, Any] | None:
        """
        Get detailed information about a ticker.

        Args:
            ticker: Stock ticker symbol

        Returns:
            Ticker details dict or None if not found
        """
        url = f"{self.base_url}/v3/reference/tickers/{ticker.upper()}"
        params = {"apiKey": self.api_key}

        async with httpx.AsyncClient(timeout=self.timeout) as client:
            self.rate_limiter.acquire()

            try:
                response = await client.get(url, params=params)
                response.raise_for_status()
                data = response.json()
                return data.get("results")
            except httpx.HTTPStatusError as e:
                if e.response.status_code == 404:
                    self.logger.warning(f"Ticker not found: {ticker}")
                    return None
                self.logger.error(f"HTTP error for {ticker}: {e}")
                raise ProviderError(
                    f"HTTP error fetching details for {ticker}: {e.response.status_code}",
                    provider="polygon",
                    original_error=e,
                ) from e
            except httpx.RequestError as e:
                self.logger.error(f"Request error for {ticker}: {e}")
                raise ProviderError(
                    f"Request error fetching details for {ticker}",
                    provider="polygon",
                    original_error=e,
                ) from e

    async def get_aggregates_batch(
        self,
        tickers: list[str],
        start_date: date,
        end_date: date,
        concurrency: int = 10,
        **kwargs: Any,
    ) -> dict[str, list[dict[str, Any]]]:
        """
        Fetch aggregates for multiple tickers concurrently.

        Args:
            tickers: List of ticker symbols
            start_date: Start date
            end_date: End date
            concurrency: Max concurrent requests (semaphore limit)
            **kwargs: Additional args passed to get_aggregates

        Returns:
            Dict mapping ticker -> list of OHLCV records
        """
        semaphore = asyncio.Semaphore(concurrency)

        async def fetch_one(ticker: str) -> tuple[str, list[dict[str, Any]]]:
            async with semaphore:
                results = await self.get_aggregates(ticker, start_date, end_date, **kwargs)
                return ticker, results

        tasks = [fetch_one(ticker) for ticker in tickers]
        results = await asyncio.gather(*tasks, return_exceptions=True)

        # Filter out exceptions and return successful results
        output: dict[str, list[dict[str, Any]]] = {}
        for result in results:
            if isinstance(result, Exception):
                self.logger.error(f"Batch fetch error: {result}")
                continue
            ticker, data = result
            output[ticker] = data

        return output
