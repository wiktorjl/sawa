# S&P 500 Data Pipeline - Implementation Plan

**Date:** 2026-02-01  
**Status:** Ready for Implementation  
**Estimated Duration:** 4-6 weeks

---

## Executive Summary

This plan implements a **Repository Pattern** architecture that abstracts data sources (Polygon.io, Massive, future providers) behind clean interfaces. The application layer (TUI, MCP, CLI) operates on **domain models** (StockPrice, NewsArticle, etc.) while deferring provider-specific implementations to repository classes.

### Key Benefits

- **Provider Agnostic**: Switch from Polygon to Yahoo Finance by changing one config line
- **Testable**: Mock repositories for unit testing
- **Consistent Interface**: Same API regardless of data source
- **Extensible**: Add new providers by implementing interfaces
- **Optimized**: Each provider can optimize internally (S3 bulk, REST, etc.)

---

## Architecture Overview

```
┌─────────────────────────────────────────────────────────────┐
│                    Application Layer                         │
│  ┌──────────────┐  ┌──────────────┐  ┌──────────────┐      │
│  │     TUI      │  │  MCP Server  │  │    CLI       │      │
│  └──────┬───────┘  └──────┬───────┘  └──────┬───────┘      │
└─────────┼─────────────────┼─────────────────┼──────────────┘
          │                 │                 │
          └─────────────────┼─────────────────┘
                            │
                            ▼
┌─────────────────────────────────────────────────────────────┐
│                 Repository Layer (Abstract)                  │
│                                                              │
│  StockPriceRepository    NewsRepository    FundamentalRepo  │
│  ├─ get_prices()         ├─ get_news()     ├─ get_income() │
│  ├─ get_latest()         └─ get_stream()   ├─ get_balance()│
│  └─ get_stream()                           └─ get_cashflow()│
│                                                              │
└─────────────────────────────────────────────────────────────┘
                            │
          ┌─────────────────┼─────────────────┐
          │                 │                 │
          ▼                 ▼                 ▼
┌─────────────────┐ ┌───────────────┐ ┌─────────────────┐
│  Polygon.io     │ │   Massive     │ │   Yahoo         │
│  Provider       │ │   Provider    │ │   Finance       │
│                 │ │               │ │   (future)      │
│ - REST API      │ │ - REST API    │ │ - Free tier     │
│ - S3 bulk       │ │ - Fundamentals│ │ - Basic prices  │
│ - Real-time     │ │ - Economy     │ │                 │
└─────────────────┘ └───────────────┘ └─────────────────┘
```

---

## Phase 1: Foundation (Week 1)

### Task 1.1: Create Domain Models

**Files:** `sp500_tools/domain/models.py`

```python
"""Domain models - provider-agnostic data structures."""

from dataclasses import dataclass, field
from datetime import date, datetime
from decimal import Decimal
from typing import Literal


@dataclass(frozen=True, slots=True)
class StockPrice:
    """Stock price data for a single day."""
    ticker: str
    date: date
    open: Decimal
    high: Decimal
    low: Decimal
    close: Decimal
    volume: int
    adjusted_close: Decimal | None = None
    
    def __post_init__(self):
        # Normalize ticker to uppercase
        object.__setattr__(self, 'ticker', self.ticker.upper())


@dataclass(frozen=True, slots=True)
class NewsArticle:
    """News article with optional sentiment."""
    id: str
    ticker: str
    title: str
    content: str | None
    published_at: datetime
    source: str
    url: str | None = None
    sentiment_score: float | None = None  # -1.0 to 1.0
    sentiment_label: Literal["positive", "negative", "neutral"] | None = None


@dataclass(frozen=True, slots=True)
class IncomeStatement:
    """Income statement data."""
    ticker: str
    period_end: date
    timeframe: Literal["quarterly", "annual"]
    fiscal_year: int
    fiscal_quarter: int | None
    
    # Revenue
    revenue: Decimal | None = None
    cost_of_revenue: Decimal | None = None
    gross_profit: Decimal | None = None
    
    # Operating
    research_development: Decimal | None = None
    selling_general_administrative: Decimal | None = None
    operating_income: Decimal | None = None
    
    # Net income
    net_income: Decimal | None = None
    
    # Per share
    basic_eps: Decimal | None = None
    diluted_eps: Decimal | None = None


@dataclass(frozen=True, slots=True)
class BalanceSheet:
    """Balance sheet data."""
    ticker: str
    period_end: date
    timeframe: Literal["quarterly", "annual"]
    
    # Assets
    total_assets: Decimal | None = None
    total_current_assets: Decimal | None = None
    cash_and_equivalents: Decimal | None = None
    
    # Liabilities
    total_liabilities: Decimal | None = None
    total_current_liabilities: Decimal | None = None
    long_term_debt: Decimal | None = None
    
    # Equity
    total_equity: Decimal | None = None
    retained_earnings: Decimal | None = None


@dataclass(frozen=True, slots=True)
class CashFlow:
    """Cash flow statement data."""
    ticker: str
    period_end: date
    timeframe: Literal["quarterly", "annual"]
    
    # Operating
    operating_cash_flow: Decimal | None = None
    
    # Investing
    capital_expenditure: Decimal | None = None
    
    # Financing
    dividends_paid: Decimal | None = None
    
    # Free cash flow (calculated)
    free_cash_flow: Decimal | None = None


@dataclass(frozen=True, slots=True)
class CompanyInfo:
    """Company overview information."""
    ticker: str
    name: str
    description: str | None = None
    sector: str | None = None
    industry: str | None = None
    market_cap: Decimal | None = None
    employees: int | None = None
    website: str | None = None
    ceo: str | None = None
    headquarters: str | None = None


@dataclass(frozen=True, slots=True)
class MarketSentiment:
    """Market sentiment for a ticker on a date."""
    ticker: str
    date: date
    overall_score: float  # -1.0 to 1.0
    volume: int
    source: str
    bullish_count: int = 0
    bearish_count: int = 0
```

### Task 1.2: Create Exception Hierarchy

**Files:** `sp500_tools/domain/exceptions.py`

```python
"""Repository and provider exceptions."""


class RepositoryError(Exception):
    """Base exception for repository errors."""
    pass


class ProviderError(RepositoryError):
    """Error from data provider (API failure, etc.)."""
    
    def __init__(self, message: str, provider: str, original_error: Exception | None = None):
        super().__init__(message)
        self.provider = provider
        self.original_error = original_error


class RateLimitError(ProviderError):
    """Rate limit exceeded."""
    
    def __init__(self, provider: str, retry_after: int | None = None):
        super().__init__(
            f"Rate limit exceeded for {provider}",
            provider
        )
        self.retry_after = retry_after


class AuthenticationError(ProviderError):
    """API key invalid or missing."""
    pass


class NotFoundError(RepositoryError):
    """Requested data not found."""
    pass


class ValidationError(RepositoryError):
    """Data validation failed."""
    pass
```

### Task 1.3: Create Repository Interfaces

**Files:** `sp500_tools/repositories/base.py`

```python
"""Abstract repository interfaces."""

from abc import ABC, abstractmethod
from collections.abc import AsyncIterator
from datetime import date
from typing import Literal

from sp500_tools.domain.models import (
    StockPrice,
    NewsArticle,
    IncomeStatement,
    BalanceSheet,
    CashFlow,
    CompanyInfo,
)


class Repository(ABC):
    """Base repository interface."""
    
    @property
    @abstractmethod
    def provider_name(self) -> str:
        """Human-readable provider name."""
        pass


class StockPriceRepository(Repository):
    """Repository for stock price data."""
    
    @property
    @abstractmethod
    def supports_historical_bulk(self) -> bool:
        """Whether provider supports efficient bulk historical download."""
        pass
    
    @abstractmethod
    async def get_prices(
        self,
        ticker: str,
        start_date: date,
        end_date: date
    ) -> list[StockPrice]:
        """
        Get daily prices for a ticker.
        
        Args:
            ticker: Stock symbol (e.g., "AAPL")
            start_date: Start date (inclusive)
            end_date: End date (inclusive)
            
        Returns:
            List of StockPrice objects, sorted by date ascending
            
        Raises:
            NotFoundError: If ticker not found
            ProviderError: If API request fails
        """
        pass
    
    @abstractmethod
    async def get_prices_stream(
        self,
        tickers: list[str],
        start_date: date,
        end_date: date
    ) -> AsyncIterator[StockPrice]:
        """
        Stream prices for multiple tickers (memory efficient).
        
        Yields prices as they arrive from provider.
        Order not guaranteed.
        """
        pass
    
    @abstractmethod
    async def get_latest_price(self, ticker: str) -> StockPrice | None:
        """Get most recent closing price."""
        pass
    
    @abstractmethod
    async def get_prices_for_date(
        self,
        tickers: list[str],
        target_date: date
    ) -> list[StockPrice]:
        """Get prices for multiple tickers on a specific date."""
        pass


class NewsRepository(Repository):
    """Repository for news data."""
    
    @abstractmethod
    async def get_news(
        self,
        ticker: str,
        limit: int = 20,
        days_back: int = 30
    ) -> list[NewsArticle]:
        """Get news articles for a ticker."""
        pass
    
    @abstractmethod
    async def get_news_stream(
        self,
        tickers: list[str]
    ) -> AsyncIterator[NewsArticle]:
        """Stream news as it arrives (for real-time)."""
        pass


class FundamentalRepository(Repository):
    """Repository for fundamental financial data."""
    
    @abstractmethod
    async def get_income_statements(
        self,
        ticker: str,
        timeframe: Literal["quarterly", "annual"],
        limit: int = 4
    ) -> list[IncomeStatement]:
        """Get income statements."""
        pass
    
    @abstractmethod
    async def get_balance_sheets(
        self,
        ticker: str,
        timeframe: Literal["quarterly", "annual"],
        limit: int = 4
    ) -> list[BalanceSheet]:
        """Get balance sheets."""
        pass
    
    @abstractmethod
    async def get_cash_flows(
        self,
        ticker: str,
        timeframe: Literal["quarterly", "annual"],
        limit: int = 4
    ) -> list[CashFlow]:
        """Get cash flow statements."""
        pass


class CompanyRepository(Repository):
    """Repository for company information."""
    
    @abstractmethod
    async def get_company_info(self, ticker: str) -> CompanyInfo | None:
        """Get company overview."""
        pass
    
    @abstractmethod
    async def search_companies(self, query: str, limit: int = 20) -> list[CompanyInfo]:
        """Search companies by name or ticker."""
        pass
```

### Task 1.4: Create In-Memory Cache

**Files:** `sp500_tools/repositories/cache.py`

```python
"""In-memory LRU cache for repository data."""

import time
from collections import OrderedDict
from dataclasses import dataclass, field
from typing import Any, TypeVar, Generic
from threading import Lock

T = TypeVar('T')


@dataclass
class CacheEntry(Generic[T]):
    """Cache entry with TTL."""
    value: T
    expires_at: float
    
    @property
    def is_expired(self) -> bool:
        return time.time() > self.expires_at


class InMemoryCache:
    """Thread-safe LRU cache with TTL."""
    
    def __init__(self, max_size: int = 1000, default_ttl_seconds: float = 300):
        """
        Initialize cache.
        
        Args:
            max_size: Maximum number of entries
            default_ttl_seconds: Default time-to-live in seconds
        """
        self.max_size = max_size
        self.default_ttl = default_ttl_seconds
        self._cache: OrderedDict[str, CacheEntry] = OrderedDict()
        self._lock = Lock()
    
    def get(self, key: str) -> Any | None:
        """Get value from cache if present and not expired."""
        with self._lock:
            entry = self._cache.get(key)
            if entry is None:
                return None
            
            if entry.is_expired:
                del self._cache[key]
                return None
            
            # Move to end (most recently used)
            self._cache.move_to_end(key)
            return entry.value
    
    def set(self, key: str, value: Any, ttl_seconds: float | None = None) -> None:
        """Set value in cache."""
        ttl = ttl_seconds if ttl_seconds is not None else self.default_ttl
        
        with self._lock:
            # Remove if exists (will be re-added at end)
            if key in self._cache:
                del self._cache[key]
            
            # Evict oldest if at capacity
            while len(self._cache) >= self.max_size:
                self._cache.popitem(last=False)
            
            expires_at = time.time() + ttl
            self._cache[key] = CacheEntry(value, expires_at)
    
    def clear(self) -> None:
        """Clear all entries."""
        with self._lock:
            self._cache.clear()
    
    def invalidate(self, pattern: str) -> int:
        """Invalidate keys matching pattern (simple substring match)."""
        with self._lock:
            keys_to_remove = [k for k in self._cache if pattern in k]
            for key in keys_to_remove:
                del self._cache[key]
            return len(keys_to_remove)


class NullCache:
    """No-op cache implementation."""
    
    def get(self, key: str) -> None:
        return None
    
    def set(self, key: str, value: Any, ttl_seconds: float | None = None) -> None:
        pass
    
    def clear(self) -> None:
        pass
    
    def invalidate(self, pattern: str) -> int:
        return 0
```

### Task 1.5: Create Rate Limiter

**Files:** `sp500_tools/repositories/rate_limiter.py`

```python
"""Rate limiting for API calls."""

import time
import asyncio
from dataclasses import dataclass
from typing import Protocol


class RateLimiter(Protocol):
    """Protocol for rate limiters."""
    
    async def acquire(self) -> None:
        """Acquire permission to make a request."""
        pass


@dataclass
class TokenBucket:
    """Token bucket rate limiter."""
    
    rate: float  # tokens per second
    capacity: float = 10.0  # max burst
    
    def __post_init__(self):
        self._tokens = self.capacity
        self._last_update = time.monotonic()
        self._lock = asyncio.Lock()
    
    async def acquire(self) -> None:
        """Wait until a token is available."""
        async with self._lock:
            while self._tokens < 1:
                # Calculate wait time
                now = time.monotonic()
                elapsed = now - self._last_update
                self._tokens = min(
                    self.capacity,
                    self._tokens + elapsed * self.rate
                )
                self._last_update = now
                
                if self._tokens < 1:
                    # Need to wait for a token
                    wait_time = (1 - self._tokens) / self.rate
                    await asyncio.sleep(wait_time)
            
            self._tokens -= 1


class NoOpRateLimiter:
    """Rate limiter that doesn't limit."""
    
    async def acquire(self) -> None:
        pass
```

---

## Phase 2: Repository Implementations (Week 1-2)

### Task 2.1: Implement Polygon Price Repository

**Files:** `sp500_tools/repositories/polygon_prices.py`

```python
"""Polygon.io price repository implementation."""

import asyncio
from datetime import date, timedelta
from decimal import Decimal
from typing import AsyncIterator

from sp500_tools.domain.models import StockPrice
from sp500_tools.domain.exceptions import ProviderError, NotFoundError
from sp500_tools.repositories.base import StockPriceRepository
from sp500_tools.repositories.cache import InMemoryCache, NullCache
from sp500_tools.repositories.rate_limiter import TokenBucket


class PolygonPriceRepository(StockPriceRepository):
    """
    Polygon.io price data provider.
    
    Uses S3 bulk for historical data, REST API for recent data.
    """
    
    def __init__(
        self,
        api_key: str,
        s3_access_key: str | None = None,
        s3_secret_key: str | None = None,
        cache: InMemoryCache | NullCache | None = None,
        rate_limit: float = 5.0,  # 5 req/sec for REST
        bulk_threshold_days: int = 30  # Use S3 for >30 days
    ):
        self.api_key = api_key
        self.cache = cache or NullCache()
        self.rate_limiter = TokenBucket(rate=rate_limit)
        self.bulk_threshold = bulk_threshold_days
        
        # Initialize clients
        from sp500_tools.api.client import PolygonClient
        from sp500_tools.api.s3 import PolygonS3Client
        
        self.rest_client = PolygonClient(api_key)
        self.s3_client = None
        if s3_access_key and s3_secret_key:
            self.s3_client = PolygonS3Client(
                access_key=s3_access_key,
                secret_key=s3_secret_key
            )
    
    @property
    def provider_name(self) -> str:
        return "polygon.io"
    
    @property
    def supports_historical_bulk(self) -> bool:
        return self.s3_client is not None
    
    async def get_prices(
        self,
        ticker: str,
        start_date: date,
        end_date: date
    ) -> list[StockPrice]:
        """Get prices, using cache or fetching from API."""
        # Check cache
        cache_key = f"prices:{ticker}:{start_date}:{end_date}"
        cached = self.cache.get(cache_key)
        if cached:
            return cached
        
        # Fetch from appropriate source
        prices = await self._fetch_prices(ticker, start_date, end_date)
        
        # Cache results
        self.cache.set(cache_key, prices, ttl_seconds=300)
        
        return prices
    
    async def _fetch_prices(
        self,
        ticker: str,
        start_date: date,
        end_date: date
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
        end_date: date
    ) -> list[StockPrice]:
        """Fetch from REST API."""
        await self.rate_limiter.acquire()
        
        try:
            # Run sync client in thread pool
            loop = asyncio.get_event_loop()
            results = await loop.run_in_executor(
                None,  # Default executor
                self.rest_client.get_aggs,
                ticker,
                1,
                "day",
                start_date.strftime("%Y-%m-%d"),
                end_date.strftime("%Y-%m-%d")
            )
            
            return [self._convert_rest_result(r, ticker) for r in results]
            
        except Exception as e:
            raise ProviderError(
                f"Failed to fetch prices for {ticker}: {e}",
                self.provider_name,
                e
            )
    
    async def _fetch_from_s3(
        self,
        ticker: str,
        start_date: date,
        end_date: date
    ) -> list[StockPrice]:
        """Fetch from S3 bulk data."""
        if not self.s3_client:
            raise ProviderError(
                "S3 client not configured",
                self.provider_name
            )
        
        try:
            # Get trading days first
            trading_days = await self._get_trading_days(start_date, end_date)
            
            prices = []
            for trading_day in trading_days:
                day_prices = await self._fetch_s3_day(trading_day, [ticker])
                prices.extend(day_prices)
            
            # Filter for just our ticker
            ticker_prices = [p for p in prices if p.ticker == ticker.upper()]
            return sorted(ticker_prices, key=lambda x: x.date)
            
        except Exception as e:
            raise ProviderError(
                f"Failed to fetch from S3 for {ticker}: {e}",
                self.provider_name,
                e
            )
    
    async def _get_trading_days(self, start: date, end: date) -> list[date]:
        """Get trading days in range."""
        # Could fetch from API or use simple logic (exclude weekends)
        days = []
        current = start
        while current <= end:
            if current.weekday() < 5:  # Monday = 0, Friday = 4
                days.append(current)
            current += timedelta(days=1)
        return days
    
    async def _fetch_s3_day(self, target_date: date, tickers: list[str]) -> list[StockPrice]:
        """Fetch S3 data for a specific day."""
        loop = asyncio.get_event_loop()
        
        # Run sync S3 download in thread pool
        records = await loop.run_in_executor(
            None,
            self.s3_client.download_and_parse,
            target_date,
            tickers
        )
        
        return [self._convert_s3_record(r) for r in records if r]
    
    def _convert_rest_result(self, result, ticker: str) -> StockPrice:
        """Convert Polygon REST result to domain model."""
        from datetime import datetime
        
        timestamp_ms = result.get("t")
        price_date = datetime.fromtimestamp(timestamp_ms / 1000).date()
        
        return StockPrice(
            ticker=ticker,
            date=price_date,
            open=Decimal(str(result.get("o", 0))),
            high=Decimal(str(result.get("h", 0))),
            low=Decimal(str(result.get("l", 0))),
            close=Decimal(str(result.get("c", 0))),
            volume=result.get("v", 0)
        )
    
    def _convert_s3_record(self, record: dict) -> StockPrice:
        """Convert S3 CSV record to domain model."""
        return StockPrice(
            ticker=record.get("symbol", "").upper(),
            date=record.get("date"),
            open=Decimal(str(record.get("open", 0))),
            high=Decimal(str(record.get("high", 0))),
            low=Decimal(str(record.get("low", 0))),
            close=Decimal(str(record.get("close", 0))),
            volume=int(record.get("volume", 0))
        )
    
    async def get_prices_stream(
        self,
        tickers: list[str],
        start_date: date,
        end_date: date
    ) -> AsyncIterator[StockPrice]:
        """Stream prices for multiple tickers."""
        days = (end_date - start_date).days
        
        if self.s3_client and days > self.bulk_threshold:
            # Stream from S3 day by day
            trading_days = await self._get_trading_days(start_date, end_date)
            for trading_day in trading_days:
                prices = await self._fetch_s3_day(trading_day, tickers)
                for price in prices:
                    if price.ticker.upper() in [t.upper() for t in tickers]:
                        yield price
        else:
            # Fetch all from REST (no streaming for REST)
            for ticker in tickers:
                prices = await self.get_prices(ticker, start_date, end_date)
                for price in prices:
                    yield price
    
    async def get_latest_price(self, ticker: str) -> StockPrice | None:
        """Get most recent price."""
        yesterday = date.today() - timedelta(days=1)
        prices = await self.get_prices(ticker, yesterday - timedelta(days=7), yesterday)
        return prices[-1] if prices else None
    
    async def get_prices_for_date(
        self,
        tickers: list[str],
        target_date: date
    ) -> list[StockPrice]:
        """Get prices for multiple tickers on one date."""
        if self.s3_client:
            prices = await self._fetch_s3_day(target_date, tickers)
            return [p for p in prices if p.ticker.upper() in [t.upper() for t in tickers]]
        else:
            # Fetch individually via REST
            results = []
            for ticker in tickers:
                prices = await self.get_prices(ticker, target_date, target_date)
                results.extend(prices)
            return results
```

### Task 2.2: Implement Massive Fundamental Repository

**Files:** `sp500_tools/repositories/massive_fundamentals.py`

```python
"""Massive API fundamental data repository."""

import asyncio
from datetime import date
from decimal import Decimal
from typing import Literal

from sp500_tools.domain.models import IncomeStatement, BalanceSheet, CashFlow
from sp500_tools.domain.exceptions import ProviderError
from sp500_tools.repositories.base import FundamentalRepository
from sp500_tools.repositories.cache import InMemoryCache, NullCache
from sp500_tools.repositories.rate_limiter import TokenBucket


class MassiveFundamentalRepository(FundamentalRepository):
    """Massive API provider for fundamental data."""
    
    def __init__(
        self,
        api_key: str,
        cache: InMemoryCache | NullCache | None = None,
        rate_limit: float = 2.0  # 2 req/sec for Massive
    ):
        self.api_key = api_key
        self.cache = cache or NullCache()
        self.rate_limiter = TokenBucket(rate=rate_limit)
        
        # Initialize client
        from sp500_tools.api.massive import MassiveClient  # Will need to create
        self.client = MassiveClient(api_key)
    
    @property
    def provider_name(self) -> str:
        return "massive"
    
    async def get_income_statements(
        self,
        ticker: str,
        timeframe: Literal["quarterly", "annual"],
        limit: int = 4
    ) -> list[IncomeStatement]:
        """Get income statements."""
        cache_key = f"income:{ticker}:{timeframe}:{limit}"
        cached = self.cache.get(cache_key)
        if cached:
            return cached
        
        await self.rate_limiter.acquire()
        
        try:
            loop = asyncio.get_event_loop()
            results = await loop.run_in_executor(
                None,
                self.client.get_fundamentals,
                "income-statements",
                ticker,
                timeframe
            )
            
            statements = [
                self._convert_income(r, ticker, timeframe)
                for r in results[:limit]
            ]
            
            self.cache.set(cache_key, statements, ttl_seconds=3600)
            return statements
            
        except Exception as e:
            raise ProviderError(
                f"Failed to fetch income statements for {ticker}: {e}",
                self.provider_name,
                e
            )
    
    def _convert_income(
        self,
        data: dict,
        ticker: str,
        timeframe: Literal["quarterly", "annual"]
    ) -> IncomeStatement:
        """Convert API response to domain model."""
        return IncomeStatement(
            ticker=ticker,
            period_end=date.fromisoformat(data.get("period_end", "2024-01-01")),
            timeframe=timeframe,
            fiscal_year=data.get("fiscal_year", 2024),
            fiscal_quarter=data.get("fiscal_quarter"),
            revenue=self._to_decimal(data.get("revenue")),
            cost_of_revenue=self._to_decimal(data.get("cost_of_revenue")),
            gross_profit=self._to_decimal(data.get("gross_profit")),
            research_development=self._to_decimal(data.get("research_and_development")),
            selling_general_administrative=self._to_decimal(data.get("selling_general_and_administrative")),
            operating_income=self._to_decimal(data.get("operating_income")),
            net_income=self._to_decimal(data.get("net_income")),
            basic_eps=self._to_decimal(data.get("basic_earnings_per_share")),
            diluted_eps=self._to_decimal(data.get("diluted_earnings_per_share"))
        )
    
    def _to_decimal(self, value) -> Decimal | None:
        """Convert value to Decimal."""
        if value is None:
            return None
        try:
            return Decimal(str(value))
        except (ValueError, TypeError):
            return None
    
    async def get_balance_sheets(
        self,
        ticker: str,
        timeframe: Literal["quarterly", "annual"],
        limit: int = 4
    ) -> list[BalanceSheet]:
        """Get balance sheets."""
        # Similar implementation...
        pass
    
    async def get_cash_flows(
        self,
        ticker: str,
        timeframe: Literal["quarterly", "annual"],
        limit: int = 4
    ) -> list[CashFlow]:
        """Get cash flow statements."""
        # Similar implementation...
        pass
```

### Task 2.3: Create Repository Factory

**Files:** `sp500_tools/repositories/factory.py`

```python
"""Factory for creating repository instances."""

from dataclasses import dataclass
from typing import TypeVar

from sp500_tools.repositories.base import (
    StockPriceRepository,
    NewsRepository,
    FundamentalRepository,
    CompanyRepository,
)
from sp500_tools.repositories.cache import InMemoryCache


@dataclass
class RepositoryConfig:
    """Configuration for repositories."""
    
    # API Keys
    polygon_api_key: str | None = None
    polygon_s3_access_key: str | None = None
    polygon_s3_secret_key: str | None = None
    massive_api_key: str | None = None
    
    # Provider selection
    default_price_provider: str = "polygon"
    default_news_provider: str = "polygon"
    default_fundamental_provider: str = "massive"
    
    # Cache settings
    cache_enabled: bool = True
    cache_max_size: int = 1000
    cache_ttl_seconds: float = 300
    
    # Rate limits
    polygon_rate_limit: float = 5.0
    massive_rate_limit: float = 2.0


class RepositoryFactory:
    """Factory for creating repository instances."""
    
    def __init__(self, config: RepositoryConfig):
        self.config = config
        self._cache = InMemoryCache(
            max_size=config.cache_max_size,
            default_ttl_seconds=config.cache_ttl_seconds
        ) if config.cache_enabled else None
        self._instances: dict[str, object] = {}
    
    def get_price_repository(self, provider: str | None = None) -> StockPriceRepository:
        """Get price repository instance."""
        provider = provider or self.config.default_price_provider
        cache_key = f"price:{provider}"
        
        if cache_key not in self._instances:
            if provider == "polygon":
                from sp500_tools.repositories.polygon_prices import PolygonPriceRepository
                
                if not self.config.polygon_api_key:
                    raise ValueError("Polygon API key not configured")
                
                self._instances[cache_key] = PolygonPriceRepository(
                    api_key=self.config.polygon_api_key,
                    s3_access_key=self.config.polygon_s3_access_key,
                    s3_secret_key=self.config.polygon_s3_secret_key,
                    cache=self._cache,
                    rate_limit=self.config.polygon_rate_limit
                )
            else:
                raise ValueError(f"Unknown price provider: {provider}")
        
        return self._instances[cache_key]
    
    def get_fundamental_repository(self, provider: str | None = None) -> FundamentalRepository:
        """Get fundamental repository instance."""
        provider = provider or self.config.default_fundamental_provider
        cache_key = f"fundamental:{provider}"
        
        if cache_key not in self._instances:
            if provider == "massive":
                from sp500_tools.repositories.massive_fundamentals import MassiveFundamentalRepository
                
                if not self.config.massive_api_key:
                    raise ValueError("Massive API key not configured")
                
                self._instances[cache_key] = MassiveFundamentalRepository(
                    api_key=self.config.massive_api_key,
                    cache=self._cache,
                    rate_limit=self.config.massive_rate_limit
                )
            else:
                raise ValueError(f"Unknown fundamental provider: {provider}")
        
        return self._instances[cache_key]
    
    def clear_cache(self) -> None:
        """Clear all repository caches."""
        if self._cache:
            self._cache.clear()


# Singleton factory instance (optional)
_factory: RepositoryFactory | None = None


def get_factory() -> RepositoryFactory:
    """Get or create the global factory instance."""
    global _factory
    if _factory is None:
        from sp500_tools.utils.config import get_config
        config = get_config()
        _factory = RepositoryFactory(config)
    return _factory


def set_factory(factory: RepositoryFactory) -> None:
    """Set the global factory instance (for testing)."""
    global _factory
    _factory = factory
```

---

## Phase 3: Configuration (Week 2)

### Task 3.1: Update Configuration Module

**Files:** `sp500_tools/utils/config.py`

```python
"""Configuration management with repository support."""

import os
from dataclasses import dataclass

from sp500_tools.repositories.factory import RepositoryConfig


def get_env(key: str, default: str | None = None, required: bool = False) -> str | None:
    """Get environment variable with optional validation."""
    value = os.environ.get(key)
    if value is None:
        if required and default is None:
            raise ValueError(f"Required environment variable {key} is not set")
        return default
    return value


def get_config() -> RepositoryConfig:
    """Create repository configuration from environment."""
    return RepositoryConfig(
        polygon_api_key=get_env("POLYGON_API_KEY"),
        polygon_s3_access_key=get_env("POLYGON_S3_ACCESS_KEY"),
        polygon_s3_secret_key=get_env("POLYGON_S3_SECRET_KEY"),
        massive_api_key=get_env("MASSIVE_API_KEY"),
        
        # Can override defaults via env vars
        default_price_provider=get_env("DEFAULT_PRICE_PROVIDER", "polygon"),
        default_fundamental_provider=get_env("DEFAULT_FUNDAMENTAL_PROVIDER", "massive"),
        
        cache_enabled=get_env("CACHE_ENABLED", "true").lower() == "true",
        cache_ttl_seconds=float(get_env("CACHE_TTL_SECONDS", "300")),
    )
```

---

## Phase 4: Migration (Week 2-3)

### Task 4.1: Migrate Coldstart

**Current:** Direct API calls in `coldstart.py`

**New:** Use repositories

```python
# sp500_tools/coldstart.py (partial)

async def download_prices(
    factory: RepositoryFactory,
    symbols: list[str],
    start_date: date,
    end_date: date,
    output_dir: Path
) -> None:
    """Download prices using repository."""
    repo = factory.get_price_repository()
    
    # Stream prices to CSV
    csv_path = output_dir / "prices.csv"
    
    with open(csv_path, "w", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(["ticker", "date", "open", "high", "low", "close", "volume"])
        
        async for price in repo.get_prices_stream(symbols, start_date, end_date):
            writer.writerow([
                price.ticker,
                price.date.isoformat(),
                price.open,
                price.high,
                price.low,
                price.close,
                price.volume
            ])
```

### Task 4.2: Migrate TUI

**Current:** Direct database queries in `tui/sp500_tui/models/queries.py`

**New:** Use repositories through a service layer

```python
# tui/sp500_tui/services/stock_service.py

from sp500_tools.repositories.factory import get_factory
from sp500_tools.domain.models import StockPrice


class StockService:
    """Service layer for stock data operations."""
    
    def __init__(self):
        self.factory = get_factory()
    
    async def get_stock_prices(
        self,
        ticker: str,
        days: int = 90
    ) -> list[StockPrice]:
        """Get prices for display."""
        repo = self.factory.get_price_repository()
        end_date = date.today()
        start_date = end_date - timedelta(days=days)
        return await repo.get_prices(ticker, start_date, end_date)
```

### Task 4.3: Migrate MCP Server

**Current:** Direct SQL queries

**New:** Can still use SQL for complex queries, but use repositories for simple fetches

```python
# mcp_server/tools/market_data.py

from sp500_tools.repositories.factory import get_factory

async def get_stock_prices(ticker: str, start_date: str, ...) -> list[dict]:
    """Get stock prices with chart."""
    factory = get_factory()
    repo = factory.get_price_repository()
    
    prices = await repo.get_prices(
        ticker,
        date.fromisoformat(start_date),
        date.fromisoformat(end_date)
    )
    
    # Convert domain models to dicts for JSON serialization
    return [
        {
            "date": p.date.isoformat(),
            "open": float(p.open),
            "close": float(p.close),
            ...
        }
        for p in prices
    ]
```

---

## Phase 5: Testing (Week 3-4)

### Task 5.1: Create Mock Repositories

**Files:** `tests/repositories/mocks.py`

```python
"""Mock repository implementations for testing."""

from datetime import date, timedelta
from decimal import Decimal

from sp500_tools.domain.models import StockPrice
from sp500_tools.repositories.base import StockPriceRepository


class MockPriceRepository(StockPriceRepository):
    """Mock price repository that returns fake data."""
    
    def __init__(self, prices: list[StockPrice] | None = None):
        self.prices = prices or []
        self.call_count = 0
    
    @property
    def provider_name(self) -> str:
        return "mock"
    
    @property
    def supports_historical_bulk(self) -> bool:
        return True
    
    async def get_prices(self, ticker, start_date, end_date):
        self.call_count += 1
        return [
            p for p in self.prices
            if p.ticker == ticker.upper()
            and start_date <= p.date <= end_date
        ]
    
    # ... other methods
```

### Task 5.2: Repository Tests

**Files:** `tests/repositories/test_polygon_prices.py`

```python
import pytest
from datetime import date
from decimal import Decimal

from sp500_tools.repositories.polygon_prices import PolygonPriceRepository
from sp500_tools.domain.models import StockPrice


@pytest.fixture
def mock_polygon_response():
    return [
        {"t": 1704067200000, "o": 150.0, "h": 155.0, "l": 149.0, "c": 153.0, "v": 1000000}
    ]


@pytest.mark.asyncio
async def test_get_prices_converts_to_domain_model(mock_polygon_response, monkeypatch):
    """Test that API response is converted to domain model."""
    # Mock the REST client
    class MockClient:
        def get_aggs(self, *args, **kwargs):
            return mock_polygon_response
    
    repo = PolygonPriceRepository(api_key="test")
    repo.rest_client = MockClient()
    
    prices = await repo.get_prices("AAPL", date(2024, 1, 1), date(2024, 1, 1))
    
    assert len(prices) == 1
    assert prices[0].ticker == "AAPL"
    assert prices[0].close == Decimal("153.0")
    assert isinstance(prices[0].date, date)
```

---

## Implementation Schedule

### Week 1: Foundation
- **Day 1-2:** Create domain models and exceptions
- **Day 3-4:** Create repository interfaces and base classes
- **Day 5:** Implement cache and rate limiter

### Week 2: Implementations
- **Day 1-2:** Implement PolygonPriceRepository
- **Day 3:** Implement MassiveFundamentalRepository
- **Day 4:** Create repository factory
- **Day 5:** Update configuration module

### Week 3: Migration
- **Day 1-2:** Migrate coldstart
- **Day 3-4:** Migrate TUI
- **Day 5:** Migrate MCP server

### Week 4: Testing & Polish
- **Day 1-2:** Create mock repositories
- **Day 3:** Write repository tests
- **Day 4:** Integration testing
- **Day 5:** Documentation and cleanup

---

## Success Criteria

1. **All API calls go through repositories** - No direct client usage in TUI/MCP/CLI
2. **Provider switching is one config change** - Change `DEFAULT_PRICE_PROVIDER=yahoo` and it works
3. **Tests use mocks** - No API calls in test suite
4. **Type safety** - Domain models are used throughout
5. **Performance maintained or improved** - Caching and pooling working

---

## Migration Checklist

- [ ] Domain models created
- [ ] Repository interfaces defined
- [ ] Polygon price repository implemented
- [ ] Massive fundamentals repository implemented
- [ ] Factory created
- [ ] Coldstart migrated
- [ ] TUI migrated
- [ ] MCP server migrated
- [ ] Old direct API calls removed
- [ ] Tests passing
- [ ] Documentation updated

---

**Ready to start! Begin with Task 1.1: Create Domain Models**
