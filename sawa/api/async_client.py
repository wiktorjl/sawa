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
from sawa.repositories.rate_limiter import RateLimiter, TokenBucket

DEFAULT_TIMEOUT = 30
MAX_REQUESTS_PER_MINUTE = 5
MAX_RETRIES = 3


class AsyncPolygonClient:
    """Async client for Polygon.io REST API."""

    def __init__(
        self,
        api_key: str,
        logger: logging.Logger,
        rate_limiter: RateLimiter | None = None,
        timeout: int = DEFAULT_TIMEOUT,
    ):
        """Initialize async client.

        Args:
            api_key: Polygon API key
            logger: Logger instance
            rate_limiter: Optional async rate limiter (must expose an async
                acquire()). Defaults to an async-safe TokenBucket so the
                event loop is not blocked by a synchronous time.sleep().
            timeout: Request timeout in seconds
        """
        self.api_key = api_key
        self.logger = logger
        self.base_url = "https://api.polygon.io"
        self.timeout = timeout
        self.rate_limiter: RateLimiter = rate_limiter or TokenBucket(
            rate=MAX_REQUESTS_PER_MINUTE / 60.0,
            capacity=float(MAX_REQUESTS_PER_MINUTE),
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
        params: dict[str, str | int | bool] = {
            "adjusted": str(adjusted).lower(),
            "sort": sort,
            "limit": limit,
            "apiKey": self.api_key,
        }

        async with httpx.AsyncClient(timeout=self.timeout) as client:
            # Retry transient failures (HTTP 429 rate limits, 5xx server
            # errors, and connection/timeout errors) with backoff, mirroring
            # the sync PolygonClient. Without this a single 429 would raise
            # immediately and silently drop the ticker from the batch result.
            for attempt in range(MAX_RETRIES):
                # Rate limiting (async-safe; awaits a TokenBucket token).
                await self.rate_limiter.acquire()

                try:
                    response = await client.get(url, params=params)

                    if response.status_code == 429:
                        wait = (attempt + 1) * 2
                        self.logger.warning(
                            f"Rate limited for {ticker}. Waiting {wait}s..."
                        )
                        await asyncio.sleep(wait)
                        continue

                    response.raise_for_status()
                    data: dict[str, Any] = response.json()
                    results: list[dict[str, Any]] = data.get("results", [])
                    return results
                except httpx.HTTPStatusError as e:
                    # Retry transient 5xx; surface other 4xx as ProviderError.
                    if e.response.status_code >= 500 and attempt < MAX_RETRIES - 1:
                        wait = attempt + 1
                        self.logger.warning(
                            f"HTTP {e.response.status_code} for {ticker}: {e}. "
                            f"Retrying in {wait}s..."
                        )
                        await asyncio.sleep(wait)
                        continue
                    self.logger.error(f"HTTP error for {ticker}: {e}")
                    raise ProviderError(
                        f"HTTP error fetching aggregates for {ticker}: "
                        f"{e.response.status_code}",
                        provider="polygon",
                        original_error=e,
                    ) from e
                except httpx.RequestError as e:
                    if attempt < MAX_RETRIES - 1:
                        wait = attempt + 1
                        self.logger.warning(
                            f"Request error for {ticker}: {e}. Retrying in {wait}s..."
                        )
                        await asyncio.sleep(wait)
                        continue
                    self.logger.error(f"Request error for {ticker}: {e}")
                    raise ProviderError(
                        f"Request error fetching aggregates for {ticker}",
                        provider="polygon",
                        original_error=e,
                    ) from e

            # All attempts exhausted on 429s without returning.
            raise ProviderError(
                f"Rate limited fetching aggregates for {ticker} "
                f"after {MAX_RETRIES} attempts",
                provider="polygon",
            )

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
            await self.rate_limiter.acquire()

            try:
                response = await client.get(url, params=params)
                response.raise_for_status()
                data: dict[str, Any] = response.json()
                result: dict[str, Any] | None = data.get("results")
                return result
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
        gathered = await asyncio.gather(*tasks, return_exceptions=True)

        # Filter out exceptions and return successful results. Surface the
        # count of dropped tickers (after the per-ticker retries above are
        # exhausted) so callers can see the gap instead of it being silent.
        output: dict[str, list[dict[str, Any]]] = {}
        dropped = 0
        for item in gathered:
            if isinstance(item, BaseException):
                dropped += 1
                self.logger.error(f"Batch fetch error: {item}")
                continue
            # item is tuple[str, list[dict[str, Any]]]
            ticker, data = item[0], item[1]
            output[ticker] = data

        if dropped:
            self.logger.warning(
                f"Batch fetch dropped {dropped}/{len(tickers)} tickers after retries"
            )

        return output
