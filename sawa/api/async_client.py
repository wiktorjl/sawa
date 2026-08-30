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
import math
from datetime import date
from typing import Any

import httpx

from sawa.domain.exceptions import ProviderError
from sawa.repositories.rate_limiter import RateLimiter, TokenBucket
from sawa.utils.security import redact_sensitive_text

DEFAULT_TIMEOUT = 30
MAX_REQUESTS_PER_MINUTE = 5
MAX_RETRIES = 3


class AggregateBatchResult(dict[str, list[dict[str, Any]]]):
    """Dictionary-compatible batch result with explicit per-ticker failures.

    Keeping this as a ``dict`` subclass preserves the pre-0.3 client API for
    callers that iterate successful ticker data, while ``failures`` prevents
    exhausted requests from disappearing without a trace.
    """

    def __init__(
        self,
        data: dict[str, list[dict[str, Any]]] | None = None,
        *,
        failures: dict[str, str] | None = None,
    ) -> None:
        super().__init__(data or {})
        self.failures = failures or {}

    def to_payload(self) -> dict[str, dict[str, Any]]:
        """Return a serialization-safe payload including failure metadata."""
        return {
            "data": dict(self),
            "failures": dict(self.failures),
        }


def _validate_aggregate_bar(item: object) -> dict[str, Any]:
    """Validate the required numeric Polygon aggregate-bar contract."""
    if not isinstance(item, dict):
        raise ValueError("provider results must contain JSON objects")

    required = ("t", "o", "h", "l", "c", "v")
    for field in required:
        value = item.get(field)
        if (
            isinstance(value, bool)
            or not isinstance(value, (int, float))
            or not math.isfinite(value)
        ):
            raise ValueError(f"provider aggregate bar has invalid {field!r} field")

    if item["t"] < 0:
        raise ValueError("provider aggregate bar timestamp must be non-negative")
    if any(item[field] <= 0 for field in ("o", "h", "l", "c")):
        raise ValueError("provider aggregate bar prices must be positive")
    if item["v"] < 0:
        raise ValueError("provider aggregate bar volume must be non-negative")
    if item["h"] < max(item["o"], item["l"], item["c"]):
        raise ValueError("provider aggregate bar high is inconsistent")
    if item["l"] > min(item["o"], item["h"], item["c"]):
        raise ValueError("provider aggregate bar low is inconsistent")

    return item


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
                        if attempt >= MAX_RETRIES - 1:
                            raise ProviderError(
                                f"Rate limited fetching aggregates for {ticker} "
                                f"after {MAX_RETRIES} attempts",
                                provider="polygon",
                            )
                        wait = (attempt + 1) * 2
                        self.logger.warning(
                            f"Rate limited for {ticker}. Waiting {wait}s..."
                        )
                        await asyncio.sleep(wait)
                        continue

                    response.raise_for_status()
                    payload = response.json()
                    if not isinstance(payload, dict):
                        raise ValueError("provider response must be a JSON object")
                    if payload.get("status") not in ("OK", "DELAYED"):
                        error = payload.get(
                            "error", payload.get("message", "Unknown provider error")
                        )
                        raise ProviderError(
                            f"Polygon API error for {ticker}: "
                            f"{redact_sensitive_text(error)}",
                            provider="polygon",
                        )
                    raw_results = payload.get("results", [])
                    if not isinstance(raw_results, list):
                        raise ValueError("provider results must be a JSON array")
                    results = [_validate_aggregate_bar(item) for item in raw_results]
                    return results
                except httpx.HTTPStatusError as e:
                    # Retry transient 5xx; surface other 4xx as ProviderError.
                    if e.response.status_code >= 500 and attempt < MAX_RETRIES - 1:
                        wait = attempt + 1
                        self.logger.warning(
                            f"HTTP {e.response.status_code} for {ticker}: "
                            f"{redact_sensitive_text(e)}. "
                            f"Retrying in {wait}s..."
                        )
                        await asyncio.sleep(wait)
                        continue
                    self.logger.error(
                        f"HTTP error for {ticker}: {redact_sensitive_text(e)}"
                    )
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
                            f"Request error for {ticker}: {redact_sensitive_text(e)}. "
                            f"Retrying in {wait}s..."
                        )
                        await asyncio.sleep(wait)
                        continue
                    self.logger.error(
                        f"Request error for {ticker}: {redact_sensitive_text(e)}"
                    )
                    raise ProviderError(
                        f"Request error fetching aggregates for {ticker}",
                        provider="polygon",
                        original_error=e,
                    ) from e
                except (TypeError, ValueError) as e:
                    if attempt < MAX_RETRIES - 1:
                        wait = attempt + 1
                        self.logger.warning(
                            f"Invalid response for {ticker}: "
                            f"{redact_sensitive_text(e)}. Retrying in {wait}s..."
                        )
                        await asyncio.sleep(wait)
                        continue
                    raise ProviderError(
                        f"Invalid response fetching aggregates for {ticker}",
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
                payload = response.json()
                if not isinstance(payload, dict):
                    raise ProviderError(
                        "Invalid ticker-details response object",
                        provider="polygon",
                    )
                if payload.get("status") not in ("OK", "DELAYED"):
                    error = payload.get(
                        "error", payload.get("message", "Unknown provider error")
                    )
                    raise ProviderError(
                        f"Polygon API error for {ticker}: "
                        f"{redact_sensitive_text(error)}",
                        provider="polygon",
                    )
                result = payload.get("results")
                if not isinstance(result, dict):
                    raise ProviderError(
                        "Invalid ticker-details results object",
                        provider="polygon",
                    )
                return result
            except httpx.HTTPStatusError as e:
                if e.response.status_code == 404:
                    self.logger.warning(f"Ticker not found: {ticker}")
                    return None
                self.logger.error(
                    f"HTTP error for {ticker}: {redact_sensitive_text(e)}"
                )
                raise ProviderError(
                    f"HTTP error fetching details for {ticker}: {e.response.status_code}",
                    provider="polygon",
                    original_error=e,
                ) from e
            except httpx.RequestError as e:
                self.logger.error(
                    f"Request error for {ticker}: {redact_sensitive_text(e)}"
                )
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
    ) -> AggregateBatchResult:
        """
        Fetch aggregates for multiple tickers concurrently.

        Args:
            tickers: List of ticker symbols
            start_date: Start date
            end_date: End date
            concurrency: Max concurrent requests (semaphore limit)
            **kwargs: Additional args passed to get_aggregates

        Returns:
            Dict-compatible result mapping successful tickers to OHLCV records.
            Its ``failures`` attribute maps failed tickers to safe error details.
        """
        if concurrency < 1:
            raise ValueError("concurrency must be at least 1")

        requested_tickers: tuple[str, ...] = tuple(
            dict.fromkeys(ticker.strip().upper() for ticker in tuple(tickers))
        )
        if any(not ticker for ticker in requested_tickers):
            raise ValueError("ticker cannot be empty")

        semaphore = asyncio.Semaphore(concurrency)

        async def fetch_one(ticker: str) -> tuple[str, list[dict[str, Any]]]:
            async with semaphore:
                results = await self.get_aggregates(ticker, start_date, end_date, **kwargs)
                return ticker, results

        tasks = [fetch_one(ticker) for ticker in requested_tickers]
        gathered = await asyncio.gather(*tasks, return_exceptions=True)

        # Preserve successful dict iteration for compatibility, but retain the
        # original ticker for every exhausted request in explicit metadata.
        output: dict[str, list[dict[str, Any]]] = {}
        failures: dict[str, str] = {}
        for requested_ticker, item in zip(requested_tickers, gathered, strict=True):
            if isinstance(item, BaseException):
                # Cancellation and other control-flow BaseExceptions must not
                # be converted into ordinary provider failures.
                if not isinstance(item, Exception):
                    raise item
                safe_error = redact_sensitive_text(
                    f"{type(item).__name__}: {item}"
                )
                failures[requested_ticker] = safe_error
                self.logger.error(
                    f"Batch fetch error for {requested_ticker}: {safe_error}"
                )
                continue
            # item is tuple[str, list[dict[str, Any]]]
            ticker, data = item[0], item[1]
            output[ticker] = data

        if failures:
            self.logger.warning(
                f"Batch fetch failed for {len(failures)}/{len(requested_tickers)} "
                "tickers after retries"
            )

        return AggregateBatchResult(output, failures=failures)
