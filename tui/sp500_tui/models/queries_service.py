"""Stock queries via service layer.

This module provides StockQueriesViaService as an alternative to
StockQueries that uses the repository-based service layer instead
of direct SQL queries.

Usage:
    from sp500_tui.models.queries_service import StockQueriesViaService

    # Use service-based queries
    company = StockQueriesViaService.get_company("AAPL")

    # Or use the factory function
    from sp500_tui.models.queries_service import get_queries
    queries = get_queries(use_service=True)
    company = queries.get_company("AAPL")
"""

import os
from datetime import date
from typing import TYPE_CHECKING, Any

from sp500_tui.models.queries import (
    BalanceSheet,
    CashFlow,
    Company,
    FinancialRatios,
    IncomeStatement,
    Inflation,
    LaborMarket,
    NewsArticle,
    ScreenerResult,
    StockPrice,
    StockQueries,
    TreasuryYields,
)

# Import services lazily to avoid circular imports
# sp500_tui.services imports from queries which imports from queries_service

if TYPE_CHECKING:
    from typing import TypeAlias

    QueriesType: TypeAlias = type[StockQueries] | type["StockQueriesViaService"]


class StockQueriesViaService:
    """Query methods for stock and market data via service layer.

    This class provides the same interface as StockQueries but uses
    the repository-based service layer instead of direct SQL queries.

    Note:
        Some methods (list_companies, get_news, get_screener_universe)
        fall back to StockQueries because they use SQL features not
        available in the repository layer.
    """

    _stock_service: Any = None  # Will be StockService instance
    _economy_service: Any = None  # Will be EconomyService instance

    @classmethod
    def _get_stock_service(cls):
        """Get or create stock service singleton."""
        if cls._stock_service is None:
            from sp500_tui.services.stock_service import StockService

            cls._stock_service = StockService()
        return cls._stock_service

    @classmethod
    def _get_economy_service(cls):
        """Get or create economy service singleton."""
        if cls._economy_service is None:
            from sp500_tui.services.economy_service import EconomyService

            cls._economy_service = EconomyService()
        return cls._economy_service

    @staticmethod
    def get_company(ticker: str) -> Company | None:
        """Get company details by ticker."""
        service = StockQueriesViaService._get_stock_service()
        return service.get_company(ticker)

    @staticmethod
    def search_companies(query: str, limit: int = 20) -> list[Company]:
        """Search companies by ticker or name."""
        service = StockQueriesViaService._get_stock_service()
        return service.search_companies(query, limit)

    @staticmethod
    def list_companies(
        sector: str | None = None,
        limit: int = 100,
        offset: int = 0,
    ) -> list[Company]:
        """List companies with optional sector filter.

        Note: Falls back to StockQueries because sector filtering
        and pagination are not available in the repository layer.
        """
        return StockQueries.list_companies(sector, limit, offset)

    @staticmethod
    def get_stock_prices(
        ticker: str,
        days: int = 60,
        start_date: date | None = None,
        end_date: date | None = None,
    ) -> list[StockPrice]:
        """Get stock prices for a ticker."""
        service = StockQueriesViaService._get_stock_service()
        return service.get_stock_prices(ticker, days, start_date, end_date)

    @staticmethod
    def get_latest_price(ticker: str) -> StockPrice | None:
        """Get the latest price for a ticker."""
        service = StockQueriesViaService._get_stock_service()
        return service.get_latest_price(ticker)

    @staticmethod
    def get_financial_ratios(ticker: str, limit: int = 10) -> list[FinancialRatios]:
        """Get financial ratios for a ticker."""
        service = StockQueriesViaService._get_stock_service()
        return service.get_financial_ratios(ticker, limit)

    @staticmethod
    def get_latest_ratios(ticker: str) -> FinancialRatios | None:
        """Get the latest financial ratios for a ticker."""
        service = StockQueriesViaService._get_stock_service()
        return service.get_latest_ratios(ticker)

    @staticmethod
    def get_income_statements(
        ticker: str,
        timeframe: str = "quarterly",
        limit: int = 8,
    ) -> list[IncomeStatement]:
        """Get income statements for a ticker."""
        service = StockQueriesViaService._get_stock_service()
        return service.get_income_statements(ticker, timeframe, limit)

    @staticmethod
    def get_balance_sheets(
        ticker: str,
        timeframe: str = "quarterly",
        limit: int = 8,
    ) -> list[BalanceSheet]:
        """Get balance sheets for a ticker."""
        service = StockQueriesViaService._get_stock_service()
        return service.get_balance_sheets(ticker, timeframe, limit)

    @staticmethod
    def get_cash_flows(
        ticker: str,
        timeframe: str = "quarterly",
        limit: int = 8,
    ) -> list[CashFlow]:
        """Get cash flow statements for a ticker."""
        service = StockQueriesViaService._get_stock_service()
        return service.get_cash_flows(ticker, timeframe, limit)

    @staticmethod
    def get_treasury_yields(limit: int = 30) -> list[TreasuryYields]:
        """Get treasury yields."""
        service = StockQueriesViaService._get_economy_service()
        return service.get_treasury_yields(limit)

    @staticmethod
    def get_inflation(limit: int = 30) -> list[Inflation]:
        """Get inflation data."""
        service = StockQueriesViaService._get_economy_service()
        return service.get_inflation(limit)

    @staticmethod
    def get_labor_market(limit: int = 30) -> list[LaborMarket]:
        """Get labor market data."""
        service = StockQueriesViaService._get_economy_service()
        return service.get_labor_market(limit)

    @staticmethod
    def get_52_week_range(ticker: str) -> tuple[float | None, float | None]:
        """Get 52-week high and low for a ticker."""
        service = StockQueriesViaService._get_stock_service()
        return service.get_52_week_range(ticker)

    @staticmethod
    def get_news(ticker: str, limit: int = 10) -> list[NewsArticle]:
        """Get news articles for a ticker with sentiment.

        Note: Falls back to StockQueries because news is not
        available in the repository layer.
        """
        return StockQueries.get_news(ticker, limit)

    @staticmethod
    def get_news_sentiment_summary(ticker: str, days: int = 30) -> dict[str, int]:
        """Get sentiment summary counts for a ticker.

        Note: Falls back to StockQueries because news is not
        available in the repository layer.
        """
        return StockQueries.get_news_sentiment_summary(ticker, days)

    @staticmethod
    def get_screener_universe() -> list[ScreenerResult]:
        """Get full universe of data for screening.

        Note: Falls back to StockQueries because this complex query
        is not available in the repository layer.
        """
        return StockQueries.get_screener_universe()


def get_queries(use_service: bool | None = None) -> "QueriesType":
    """Get the appropriate queries class.

    Args:
        use_service: If True, use service-based queries. If False, use
                    direct SQL queries. If None, check TUI_USE_SERVICE_LAYER
                    environment variable (default: False).

    Returns:
        StockQueries or StockQueriesViaService class
    """
    if use_service is None:
        use_service = os.environ.get("TUI_USE_SERVICE_LAYER", "").lower() in ("1", "true", "yes")

    if use_service:
        return StockQueriesViaService
    return StockQueries
