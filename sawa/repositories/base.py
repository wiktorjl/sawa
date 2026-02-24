"""Abstract repository interfaces.

This module defines the abstract base classes for all repositories.
Concrete implementations (database, Polygon, etc.) must implement
these interfaces.

Design Principles:
    - All methods are async for consistency
    - Methods return domain models, not raw dicts
    - Exceptions are documented in docstrings
    - Streaming methods use AsyncIterator for memory efficiency

Usage:
    class MyPriceRepository(StockPriceRepository):
        async def get_prices(self, ticker, start_date, end_date):
            # Implementation here
            pass
"""

from abc import ABC, abstractmethod
from collections.abc import AsyncIterator
from datetime import date
from typing import Literal

from sawa.domain.models import (
    BalanceSheet,
    CashFlow,
    CompanyInfo,
    FinancialRatio,
    IncomeStatement,
    InflationData,
    LaborMarketData,
    MarketIndex,
    NewsArticle,
    StockPrice,
    TreasuryYield,
)
from sawa.domain.technical_indicators import TechnicalIndicators


class Repository(ABC):
    """Base repository interface.

    All repositories must implement this interface which provides
    common functionality like provider identification.
    """

    @property
    @abstractmethod
    def provider_name(self) -> str:
        """Human-readable provider name (e.g., 'database', 'polygon.io')."""
        pass


class StockPriceRepository(Repository):
    """Repository for stock price data.

    Provides methods to fetch historical and current stock prices.
    Implementations may optimize for different access patterns
    (single ticker vs bulk, recent vs historical).
    """

    @property
    @abstractmethod
    def supports_historical_bulk(self) -> bool:
        """Whether provider supports efficient bulk historical download.

        Returns True if the provider can efficiently fetch large amounts
        of historical data (e.g., S3 bulk files, database queries).
        """
        pass

    @abstractmethod
    async def get_prices(
        self,
        ticker: str,
        start_date: date,
        end_date: date,
    ) -> list[StockPrice]:
        """Get daily prices for a ticker.

        Args:
            ticker: Stock symbol (e.g., "AAPL")
            start_date: Start date (inclusive)
            end_date: End date (inclusive)

        Returns:
            List of StockPrice objects, sorted by date ascending

        Raises:
            NotFoundError: If ticker not found
            ProviderError: If provider request fails
        """
        pass

    @abstractmethod
    def get_prices_stream(
        self,
        tickers: list[str],
        start_date: date,
        end_date: date,
    ) -> AsyncIterator[StockPrice]:
        """Stream prices for multiple tickers (memory efficient).

        Yields prices as they arrive from provider.
        Order is not guaranteed.

        Note: Implementations should use `async def` with `yield` to create
        an async generator. The return type is AsyncIterator for type checking.

        Args:
            tickers: List of stock symbols
            start_date: Start date (inclusive)
            end_date: End date (inclusive)

        Yields:
            StockPrice objects as they become available

        Raises:
            ProviderError: If provider request fails
        """
        ...

    @abstractmethod
    async def get_latest_price(self, ticker: str) -> StockPrice | None:
        """Get most recent closing price.

        Args:
            ticker: Stock symbol

        Returns:
            Most recent StockPrice, or None if not found
        """
        pass

    @abstractmethod
    async def get_prices_for_date(
        self,
        tickers: list[str],
        target_date: date,
    ) -> list[StockPrice]:
        """Get prices for multiple tickers on a specific date.

        Args:
            tickers: List of stock symbols
            target_date: The date to fetch prices for

        Returns:
            List of StockPrice objects for tickers that have data
        """
        pass


class NewsRepository(Repository):
    """Repository for news data.

    Provides methods to fetch news articles and sentiment data
    for stocks.
    """

    @abstractmethod
    async def get_news(
        self,
        ticker: str,
        limit: int = 20,
        days_back: int = 30,
    ) -> list[NewsArticle]:
        """Get news articles for a ticker.

        Args:
            ticker: Stock symbol
            limit: Maximum number of articles to return
            days_back: Number of days to look back

        Returns:
            List of NewsArticle objects, sorted by date descending
        """
        pass

    @abstractmethod
    def get_news_stream(
        self,
        tickers: list[str],
    ) -> AsyncIterator[NewsArticle]:
        """Stream news as it arrives (for real-time).

        Note: Implementations should use `async def` with `yield` to create
        an async generator.

        Args:
            tickers: List of stock symbols to monitor

        Yields:
            NewsArticle objects as they become available
        """
        ...


class FundamentalRepository(Repository):
    """Repository for fundamental financial data.

    Provides methods to fetch financial statements including
    income statements, balance sheets, and cash flow statements.
    """

    @abstractmethod
    async def get_income_statements(
        self,
        ticker: str,
        timeframe: Literal["quarterly", "annual"],
        limit: int = 4,
    ) -> list[IncomeStatement]:
        """Get income statements.

        Args:
            ticker: Stock symbol
            timeframe: "quarterly" or "annual"
            limit: Maximum number of periods to return

        Returns:
            List of IncomeStatement objects, sorted by period_end descending
        """
        pass

    @abstractmethod
    async def get_balance_sheets(
        self,
        ticker: str,
        timeframe: Literal["quarterly", "annual"],
        limit: int = 4,
    ) -> list[BalanceSheet]:
        """Get balance sheets.

        Args:
            ticker: Stock symbol
            timeframe: "quarterly" or "annual"
            limit: Maximum number of periods to return

        Returns:
            List of BalanceSheet objects, sorted by period_end descending
        """
        pass

    @abstractmethod
    async def get_cash_flows(
        self,
        ticker: str,
        timeframe: Literal["quarterly", "annual"],
        limit: int = 4,
    ) -> list[CashFlow]:
        """Get cash flow statements.

        Args:
            ticker: Stock symbol
            timeframe: "quarterly" or "annual"
            limit: Maximum number of periods to return

        Returns:
            List of CashFlow objects, sorted by period_end descending
        """
        pass


class CompanyRepository(Repository):
    """Repository for company information.

    Provides methods to fetch company metadata and search
    for companies.
    """

    @abstractmethod
    async def get_company_info(self, ticker: str) -> CompanyInfo | None:
        """Get company overview.

        Args:
            ticker: Stock symbol

        Returns:
            CompanyInfo object, or None if not found
        """
        pass

    @abstractmethod
    async def search_companies(
        self,
        query: str,
        limit: int = 20,
    ) -> list[CompanyInfo]:
        """Search companies by name or ticker.

        Args:
            query: Search query (matches ticker or name)
            limit: Maximum number of results

        Returns:
            List of matching CompanyInfo objects
        """
        pass


class RatiosRepository(Repository):
    """Repository for financial ratios data.

    Provides methods to fetch calculated financial ratios
    like P/E, ROE, debt ratios, etc.
    """

    @abstractmethod
    async def get_ratios(
        self,
        ticker: str,
        start_date: date,
        end_date: date,
    ) -> list[FinancialRatio]:
        """Get financial ratios for a ticker.

        Args:
            ticker: Stock symbol
            start_date: Start date (inclusive)
            end_date: End date (inclusive)

        Returns:
            List of FinancialRatio objects, sorted by date ascending
        """
        pass

    @abstractmethod
    async def get_latest_ratio(self, ticker: str) -> FinancialRatio | None:
        """Get most recent ratio for a ticker.

        Args:
            ticker: Stock symbol

        Returns:
            Most recent FinancialRatio, or None if not found
        """
        pass


class EconomyRepository(Repository):
    """Repository for economic indicator data.

    Provides methods to fetch macroeconomic data like
    treasury yields, inflation, and labor market indicators.
    """

    @abstractmethod
    async def get_treasury_yields(
        self,
        start_date: date,
        end_date: date,
    ) -> list[TreasuryYield]:
        """Get treasury yield data.

        Args:
            start_date: Start date (inclusive)
            end_date: End date (inclusive)

        Returns:
            List of TreasuryYield objects, sorted by date ascending
        """
        pass

    @abstractmethod
    async def get_inflation(
        self,
        start_date: date,
        end_date: date,
        indicator: str | None = None,
    ) -> list[InflationData]:
        """Get inflation data.

        Args:
            start_date: Start date (inclusive)
            end_date: End date (inclusive)
            indicator: Optional filter for specific indicator (CPI, PCE, etc.)

        Returns:
            List of InflationData objects, sorted by date ascending
        """
        pass

    @abstractmethod
    async def get_labor_market(
        self,
        start_date: date,
        end_date: date,
        indicator: str | None = None,
    ) -> list[LaborMarketData]:
        """Get labor market data.

        Args:
            start_date: Start date (inclusive)
            end_date: End date (inclusive)
            indicator: Optional filter for specific indicator

        Returns:
            List of LaborMarketData objects, sorted by date ascending
        """
        pass


class TechnicalIndicatorsRepository(Repository):
    """Repository for technical indicator data.

    Provides methods to fetch technical indicators (SMA, RSI, MACD, etc.)
    calculated from OHLCV price data.
    """

    @abstractmethod
    async def get_indicators(
        self,
        ticker: str,
        start_date: date,
        end_date: date,
    ) -> list[TechnicalIndicators]:
        """Get technical indicators for a ticker.

        Args:
            ticker: Stock symbol
            start_date: Start date (inclusive)
            end_date: End date (inclusive)

        Returns:
            List of TechnicalIndicators objects, sorted by date ascending
        """
        pass

    @abstractmethod
    async def get_latest_indicators(self, ticker: str) -> TechnicalIndicators | None:
        """Get most recent technical indicators for a ticker.

        Args:
            ticker: Stock symbol

        Returns:
            Most recent TechnicalIndicators, or None if not found
        """
        pass

    @abstractmethod
    async def screen_by_indicators(
        self,
        filters: dict[str, tuple[float | None, float | None]],
        target_date: date | None = None,
        index: str | None = None,
        limit: int = 100,
    ) -> list[TechnicalIndicators]:
        """Screen stocks by technical indicator values.

        Args:
            filters: Dict mapping indicator name to (min, max) tuple.
                     Use None for unbounded side.
                     Example: {"rsi_14": (None, 30), "volume_ratio": (1.5, None)}
            target_date: Date to screen (defaults to most recent)
            index: Filter by index membership (sp500, nasdaq5000)
            limit: Maximum number of results

        Returns:
            List of TechnicalIndicators matching all filters
        """
        pass


class IndexRepository(Repository):
    """Repository for market index data.

    Provides methods to query market indices (S&P 500, NASDAQ-100, etc.)
    and their constituent stocks.
    """

    @abstractmethod
    async def list_indices(self) -> list[MarketIndex]:
        """List all available market indices.

        Returns:
            List of MarketIndex objects with constituent counts
        """
        pass

    @abstractmethod
    async def get_index(self, code: str) -> MarketIndex | None:
        """Get a specific index by code.

        Args:
            code: Index code (e.g., 'sp500', 'nasdaq5000')

        Returns:
            MarketIndex object, or None if not found
        """
        pass

    @abstractmethod
    async def get_constituents(self, code: str) -> list[str]:
        """Get all tickers in an index.

        Args:
            code: Index code (e.g., 'sp500', 'nasdaq5000')

        Returns:
            List of ticker symbols in the index
        """
        pass

    @abstractmethod
    async def is_member(self, ticker: str, index_code: str) -> bool:
        """Check if a ticker is a member of an index.

        Args:
            ticker: Stock symbol
            index_code: Index code

        Returns:
            True if ticker is in the index
        """
        pass

    @abstractmethod
    async def get_ticker_indices(self, ticker: str) -> list[str]:
        """Get all indices a ticker belongs to.

        Args:
            ticker: Stock symbol

        Returns:
            List of index codes the ticker is a member of
        """
        pass

    @abstractmethod
    async def update_constituents(self, code: str, tickers: list[str]) -> int:
        """Update index constituents (replace existing).

        Args:
            code: Index code
            tickers: List of ticker symbols to set as constituents

        Returns:
            Number of constituents added

        Raises:
            ValueError: If index code not found
        """
        pass
