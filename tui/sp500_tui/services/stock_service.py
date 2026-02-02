"""Stock service - synchronous wrapper around async repositories.

This service provides synchronous methods for accessing stock data
via the repository layer. It handles async-to-sync conversion using
asyncio.run() and converts domain models to TUI dataclasses.

Usage:
    from sp500_tui.services import StockService

    service = StockService()
    company = service.get_company("AAPL")
    prices = service.get_prices("AAPL", days=60)
    ratios = service.get_financial_ratios("AAPL", limit=10)
"""

import asyncio
from datetime import date, timedelta

from sp500_tools.repositories import get_factory

from sp500_tui.models.queries import (
    BalanceSheet,
    CashFlow,
    Company,
    FinancialRatios,
    IncomeStatement,
    StockPrice,
)
from sp500_tui.services.converters import (
    balance_sheet_to_tui,
    cash_flow_to_tui,
    company_info_to_tui,
    financial_ratio_to_tui,
    income_statement_to_tui,
    stock_price_to_tui,
)


class StockService:
    """Synchronous service for stock data access.

    This service wraps the async repository layer and provides
    synchronous methods compatible with the TUI's synchronous
    architecture.

    Attributes:
        _factory: Repository factory instance

    Example:
        service = StockService()
        company = service.get_company("AAPL")
        if company:
            print(f"{company.name}: {company.sector}")
    """

    def __init__(self) -> None:
        """Initialize the service with repository factory."""
        self._factory = get_factory()

    def _run_async(self, coro):
        """Run an async coroutine synchronously.

        Args:
            coro: Coroutine to execute

        Returns:
            Result of the coroutine
        """
        return asyncio.run(coro)

    def get_company(self, ticker: str) -> Company | None:
        """Get company details by ticker.

        Args:
            ticker: Stock symbol

        Returns:
            Company dataclass, or None if not found
        """
        repo = self._factory.get_company_repository()
        result = self._run_async(repo.get_company_info(ticker))
        return company_info_to_tui(result) if result else None

    def search_companies(self, query: str, limit: int = 20) -> list[Company]:
        """Search companies by ticker or name.

        Args:
            query: Search query (matches ticker or name)
            limit: Maximum number of results

        Returns:
            List of matching Company objects
        """
        repo = self._factory.get_company_repository()
        results = self._run_async(repo.search_companies(query, limit))
        return [company_info_to_tui(r) for r in results]

    def get_stock_prices(
        self,
        ticker: str,
        days: int = 60,
        start_date: date | None = None,
        end_date: date | None = None,
    ) -> list[StockPrice]:
        """Get stock prices for a ticker.

        Args:
            ticker: Stock symbol
            days: Number of days of history (used if start_date/end_date not provided)
            start_date: Start date (inclusive)
            end_date: End date (inclusive)

        Returns:
            List of StockPrice objects, sorted by date descending
        """
        if start_date is None:
            end_date = end_date or date.today()
            start_date = end_date - timedelta(days=days)
        elif end_date is None:
            end_date = date.today()

        repo = self._factory.get_price_repository()
        results = self._run_async(repo.get_prices(ticker, start_date, end_date))

        # Convert and reverse to match TUI's expected order (desc)
        prices = [stock_price_to_tui(r) for r in results]
        return list(reversed(prices))

    def get_latest_price(self, ticker: str) -> StockPrice | None:
        """Get the latest price for a ticker.

        Args:
            ticker: Stock symbol

        Returns:
            Most recent StockPrice, or None if not found
        """
        repo = self._factory.get_price_repository()
        result = self._run_async(repo.get_latest_price(ticker))
        return stock_price_to_tui(result) if result else None

    def get_financial_ratios(self, ticker: str, limit: int = 10) -> list[FinancialRatios]:
        """Get financial ratios for a ticker.

        Args:
            ticker: Stock symbol
            limit: Maximum number of records

        Returns:
            List of FinancialRatios objects, sorted by date descending
        """
        # Calculate date range (go back ~2 years to get enough data)
        end_date = date.today()
        start_date = end_date - timedelta(days=730)

        repo = self._factory.get_ratios_repository()
        results = self._run_async(repo.get_ratios(ticker, start_date, end_date))

        # Convert, reverse to desc order, and limit
        ratios = [financial_ratio_to_tui(r) for r in results]
        return list(reversed(ratios))[:limit]

    def get_latest_ratios(self, ticker: str) -> FinancialRatios | None:
        """Get the latest financial ratios for a ticker.

        Args:
            ticker: Stock symbol

        Returns:
            Most recent FinancialRatios, or None if not found
        """
        repo = self._factory.get_ratios_repository()
        result = self._run_async(repo.get_latest_ratios(ticker))
        return financial_ratio_to_tui(result) if result else None

    def get_income_statements(
        self,
        ticker: str,
        timeframe: str = "quarterly",
        limit: int = 8,
    ) -> list[IncomeStatement]:
        """Get income statements for a ticker.

        Args:
            ticker: Stock symbol
            timeframe: "quarterly" or "annual"
            limit: Maximum number of periods

        Returns:
            List of IncomeStatement objects, sorted by period_end descending
        """
        repo = self._factory.get_fundamental_repository()
        results = self._run_async(repo.get_income_statements(ticker, timeframe, limit))
        return [income_statement_to_tui(r) for r in results]

    def get_balance_sheets(
        self,
        ticker: str,
        timeframe: str = "quarterly",
        limit: int = 8,
    ) -> list[BalanceSheet]:
        """Get balance sheets for a ticker.

        Args:
            ticker: Stock symbol
            timeframe: "quarterly" or "annual"
            limit: Maximum number of periods

        Returns:
            List of BalanceSheet objects, sorted by period_end descending
        """
        repo = self._factory.get_fundamental_repository()
        results = self._run_async(repo.get_balance_sheets(ticker, timeframe, limit))
        return [balance_sheet_to_tui(r) for r in results]

    def get_cash_flows(
        self,
        ticker: str,
        timeframe: str = "quarterly",
        limit: int = 8,
    ) -> list[CashFlow]:
        """Get cash flow statements for a ticker.

        Args:
            ticker: Stock symbol
            timeframe: "quarterly" or "annual"
            limit: Maximum number of periods

        Returns:
            List of CashFlow objects, sorted by period_end descending
        """
        repo = self._factory.get_fundamental_repository()
        results = self._run_async(repo.get_cash_flows(ticker, timeframe, limit))
        return [cash_flow_to_tui(r) for r in results]

    def get_52_week_range(self, ticker: str) -> tuple[float | None, float | None]:
        """Get 52-week high and low for a ticker.

        Args:
            ticker: Stock symbol

        Returns:
            Tuple of (high_52w, low_52w), with None values if not available
        """
        end_date = date.today()
        start_date = end_date - timedelta(weeks=52)

        repo = self._factory.get_price_repository()
        prices = self._run_async(repo.get_prices(ticker, start_date, end_date))

        if not prices:
            return None, None

        high = max(float(p.high) for p in prices)
        low = min(float(p.low) for p in prices)

        return high, low
