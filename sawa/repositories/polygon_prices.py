"""Polygon.io price repository implementation.

This module provides a repository implementation that fetches stock prices
from Polygon.io using either their REST API (for recent/small date ranges)
or S3 bulk files (for historical/large date ranges).

Usage:
    from sawa.repositories.polygon_prices import PolygonPriceRepository

    repo = PolygonPriceRepository(
        api_key="your_api_key",
        s3_access_key="s3_key",
        s3_secret_key="s3_secret",
    )
    prices = await repo.get_prices("AAPL", start_date, end_date)
"""

import asyncio
import logging
from collections.abc import AsyncIterator
from datetime import date, datetime, timedelta
from decimal import Decimal
from typing import Any

from sawa.domain.exceptions import ProviderError
from sawa.domain.models import StockPrice
from sawa.repositories.base import StockPriceRepository
from sawa.repositories.cache import InMemoryCache, NullCache
from sawa.repositories.rate_limiter import TokenBucket

logger = logging.getLogger(__name__)


class PolygonPriceRepository(StockPriceRepository):
    """Polygon.io price data provider.

    This repository uses the Polygon REST API for recent data and small
    date ranges, and S3 bulk files for historical data and large ranges.

    The threshold for switching between REST and S3 is configurable via
    the `bulk_threshold_days` parameter.

    Attributes:
        api_key: Polygon.io API key
        cache: Cache instance for storing results
        rate_limiter: Rate limiter for API calls
        bulk_threshold: Days threshold for using S3 vs REST
    """

    def __init__(
        self,
        api_key: str,
        s3_access_key: str | None = None,
        s3_secret_key: str | None = None,
        cache: InMemoryCache | NullCache | None = None,
        rate_limit: float = 5.0,
        bulk_threshold_days: int = 30,
    ) -> None:
        """Initialize repository.

        Args:
            api_key: Polygon.io REST API key
            s3_access_key: Polygon S3 access key (optional)
            s3_secret_key: Polygon S3 secret key (optional)
            cache: Cache instance (uses NullCache if not provided)
            rate_limit: Requests per second for REST API
            bulk_threshold_days: Use S3 for ranges larger than this
        """
        self.api_key = api_key
        self.cache = cache or NullCache()
        self.rate_limiter = TokenBucket(rate=rate_limit)
        self.bulk_threshold = bulk_threshold_days

        # Lazy-load clients to avoid import overhead
        self._rest_client: Any = None
        self._s3_client: Any = None
        self._s3_access_key = s3_access_key
        self._s3_secret_key = s3_secret_key

    @property
    def rest_client(self) -> Any:
        """Lazy-load REST client."""
        if self._rest_client is None:
            from sawa.api.client import PolygonClient

            self._rest_client = PolygonClient(self.api_key, logger=logger)
        return self._rest_client

    @property
    def s3_client(self) -> Any | None:
        """Lazy-load S3 client."""
        if self._s3_client is None and self._s3_access_key and self._s3_secret_key:
            from sawa.api.s3 import PolygonS3Client

            self._s3_client = PolygonS3Client(
                access_key=self._s3_access_key,
                secret_key=self._s3_secret_key,
                logger=logger,
            )
        return self._s3_client

    @property
    def provider_name(self) -> str:
        """Return provider name."""
        return "polygon.io"

    @property
    def supports_historical_bulk(self) -> bool:
        """Return True if S3 client is configured."""
        return self._s3_access_key is not None and self._s3_secret_key is not None

    async def get_prices(
        self,
        ticker: str,
        start_date: date,
        end_date: date,
    ) -> list[StockPrice]:
        """Get prices, using cache or fetching from API.

        Args:
            ticker: Stock symbol
            start_date: Start date (inclusive)
            end_date: End date (inclusive)

        Returns:
            List of StockPrice objects, sorted by date ascending

        Raises:
            ProviderError: If API request fails
        """
        # Check cache first
        cache_key = f"{self.provider_name}:prices:{ticker}:{start_date}:{end_date}"
        cached = self.cache.get(cache_key)
        if cached is not None:
            return cached

        # Fetch from appropriate source
        prices = await self._fetch_prices(ticker, start_date, end_date)

        # Cache results (5 minutes for recent data, longer for historical)
        days_old = (date.today() - end_date).days
        ttl = 3600 if days_old > 7 else 300  # 1 hour for old data, 5 min for recent
        self.cache.set(cache_key, prices, ttl_seconds=ttl)

        return prices

    async def _fetch_prices(
        self,
        ticker: str,
        start_date: date,
        end_date: date,
    ) -> list[StockPrice]:
        """Fetch from S3 or REST based on date range."""
        days = (end_date - start_date).days

        if self.s3_client and days > self.bulk_threshold:
            return await self._fetch_from_s3(ticker, start_date, end_date)
        else:
            return await self._fetch_from_rest(ticker, start_date, end_date)

    async def _fetch_from_rest(
        self,
        ticker: str,
        start_date: date,
        end_date: date,
    ) -> list[StockPrice]:
        """Fetch from REST API."""
        await self.rate_limiter.acquire()

        try:
            loop = asyncio.get_event_loop()
            data = await loop.run_in_executor(
                None,
                lambda: self.rest_client.get(
                    "aggregates",
                    path_params={
                        "ticker": ticker.upper(),
                        "start": start_date.strftime("%Y-%m-%d"),
                        "end": end_date.strftime("%Y-%m-%d"),
                    },
                    params={"adjusted": "true"},
                ),
            )

            results = data.get("results", [])
            return [self._convert_rest_result(r, ticker) for r in results]

        except Exception as e:
            raise ProviderError(
                f"Failed to fetch prices for {ticker}: {e}",
                self.provider_name,
                e,
            ) from e

    async def _fetch_from_s3(
        self,
        ticker: str,
        start_date: date,
        end_date: date,
    ) -> list[StockPrice]:
        """Fetch from S3 bulk data."""
        if not self.s3_client:
            raise ProviderError(
                "S3 client not configured",
                self.provider_name,
            )

        try:
            # Get trading days (simple weekday filter)
            trading_days = self._get_trading_days(start_date, end_date)

            prices: list[StockPrice] = []
            ticker_upper = ticker.upper()

            for trading_day in trading_days:
                day_prices = await self._fetch_s3_day(trading_day, {ticker_upper})
                for price in day_prices:
                    if price.ticker == ticker_upper:
                        prices.append(price)

            return sorted(prices, key=lambda x: x.date)

        except Exception as e:
            raise ProviderError(
                f"Failed to fetch from S3 for {ticker}: {e}",
                self.provider_name,
                e,
            ) from e

    def _get_trading_days(self, start: date, end: date) -> list[date]:
        """Get trading days in range (excludes weekends).

        Note: This is a simple implementation that doesn't account for
        market holidays. S3 downloads will simply return empty for
        non-trading days.
        """
        days = []
        current = start
        while current <= end:
            if current.weekday() < 5:  # Monday = 0, Friday = 4
                days.append(current)
            current += timedelta(days=1)
        return days

    async def _fetch_s3_day(
        self,
        target_date: date,
        tickers: set[str],
    ) -> list[StockPrice]:
        """Fetch S3 data for a specific day."""
        loop = asyncio.get_event_loop()

        records = await loop.run_in_executor(
            None,
            lambda: self.s3_client.download_and_parse(target_date, tickers),
        )

        return [self._convert_s3_record(r, target_date) for r in records if r.get("symbol")]

    def _convert_rest_result(self, result: dict[str, Any], ticker: str) -> StockPrice:
        """Convert Polygon REST result to domain model."""
        timestamp_ms = result.get("t", 0)
        price_date = datetime.fromtimestamp(timestamp_ms / 1000).date()

        return StockPrice(
            ticker=ticker,
            date=price_date,
            open=Decimal(str(result.get("o", 0))),
            high=Decimal(str(result.get("h", 0))),
            low=Decimal(str(result.get("l", 0))),
            close=Decimal(str(result.get("c", 0))),
            volume=int(result.get("v", 0)),
        )

    def _convert_s3_record(self, record: dict[str, Any], target_date: date) -> StockPrice:
        """Convert S3 CSV record to domain model."""
        return StockPrice(
            ticker=record.get("symbol", "").upper(),
            date=target_date,
            open=Decimal(str(record.get("open", 0) or 0)),
            high=Decimal(str(record.get("high", 0) or 0)),
            low=Decimal(str(record.get("low", 0) or 0)),
            close=Decimal(str(record.get("close", 0) or 0)),
            volume=int(record.get("volume", 0) or 0),
        )

    async def get_prices_stream(
        self,
        tickers: list[str],
        start_date: date,
        end_date: date,
    ) -> AsyncIterator[StockPrice]:
        """Stream prices for multiple tickers.

        For S3, streams day by day. For REST, fetches each ticker
        sequentially.
        """
        days = (end_date - start_date).days
        tickers_upper = {t.upper() for t in tickers}

        if self.s3_client and days > self.bulk_threshold:
            # Stream from S3 day by day
            trading_days = self._get_trading_days(start_date, end_date)
            for trading_day in trading_days:
                prices = await self._fetch_s3_day(trading_day, tickers_upper)
                for price in prices:
                    if price.ticker in tickers_upper:
                        yield price
        else:
            # Fetch all from REST (no streaming for REST)
            for ticker in tickers:
                prices = await self.get_prices(ticker, start_date, end_date)
                for price in prices:
                    yield price

    async def get_latest_price(self, ticker: str) -> StockPrice | None:
        """Get most recent price.

        Fetches the last 7 days and returns the most recent.
        """
        end_date = date.today()
        start_date = end_date - timedelta(days=7)
        prices = await self.get_prices(ticker, start_date, end_date)
        return prices[-1] if prices else None

    async def get_prices_for_date(
        self,
        tickers: list[str],
        target_date: date,
    ) -> list[StockPrice]:
        """Get prices for multiple tickers on one date."""
        tickers_upper = {t.upper() for t in tickers}

        if self.s3_client:
            prices = await self._fetch_s3_day(target_date, tickers_upper)
            return [p for p in prices if p.ticker in tickers_upper]
        else:
            # Fetch individually via REST
            results: list[StockPrice] = []
            for ticker in tickers:
                prices = await self.get_prices(ticker, target_date, target_date)
                results.extend(prices)
            return results
