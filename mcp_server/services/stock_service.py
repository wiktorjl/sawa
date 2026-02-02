"""Stock service for MCP server using repository layer.

This service provides async methods for accessing stock data
via the repository layer and converts domain models to MCP-compatible dicts.
"""

from datetime import date
from typing import Any

from sawa.repositories import get_factory

from mcp_server.services.converters import (
    balance_sheet_to_dict,
    cash_flow_to_dict,
    company_info_to_dict,
    company_to_list_dict,
    financial_ratio_to_dict,
    income_statement_to_dict,
    stock_price_to_dict,
)


class StockService:
    """Async service for stock data access via repository layer.

    This service wraps the repository layer and provides async methods
    that return MCP-compatible dictionaries.

    Example:
        service = StockService()
        prices = await service.get_prices("AAPL", "2024-01-01", "2024-01-31")
    """

    def __init__(self) -> None:
        """Initialize with repository factory."""
        self._factory = get_factory()

    async def get_prices(
        self,
        ticker: str,
        start_date: str,
        end_date: str | None = None,
        limit: int = 252,
    ) -> list[dict[str, Any]]:
        """Get stock prices for a ticker.

        Args:
            ticker: Stock symbol
            start_date: Start date (YYYY-MM-DD)
            end_date: End date (YYYY-MM-DD), defaults to today
            limit: Maximum rows (default: 252)

        Returns:
            List of price dicts matching MCP format
        """
        start = date.fromisoformat(start_date)
        end = date.fromisoformat(end_date) if end_date else date.today()

        repo = self._factory.get_price_repository()
        prices = await repo.get_prices(ticker, start, end)

        # Convert to dicts and limit
        result = [stock_price_to_dict(p) for p in prices]
        return result[:limit]

    async def get_latest_price(self, ticker: str) -> dict[str, Any] | None:
        """Get most recent price for a ticker.

        Args:
            ticker: Stock symbol

        Returns:
            Price dict or None
        """
        repo = self._factory.get_price_repository()
        price = await repo.get_latest_price(ticker)
        return stock_price_to_dict(price) if price else None

    async def get_financial_ratios(
        self,
        ticker: str,
        start_date: str,
        end_date: str | None = None,
        limit: int = 100,
    ) -> list[dict[str, Any]]:
        """Get financial ratios for a ticker.

        Args:
            ticker: Stock symbol
            start_date: Start date (YYYY-MM-DD)
            end_date: End date (YYYY-MM-DD), defaults to today
            limit: Maximum rows (default: 100)

        Returns:
            List of ratio dicts matching MCP format
        """
        start = date.fromisoformat(start_date)
        end = date.fromisoformat(end_date) if end_date else date.today()

        repo = self._factory.get_ratios_repository()
        ratios = await repo.get_ratios(ticker, start, end)

        result = [financial_ratio_to_dict(r) for r in ratios]
        return result[:limit]

    async def get_company_info(self, ticker: str) -> dict[str, Any] | None:
        """Get company details.

        Args:
            ticker: Stock symbol

        Returns:
            Company dict or None
        """
        repo = self._factory.get_company_repository()
        company = await repo.get_company_info(ticker)
        return company_info_to_dict(company) if company else None

    async def search_companies(
        self,
        query: str,
        limit: int = 20,
    ) -> list[dict[str, Any]]:
        """Search companies by name or ticker.

        Args:
            query: Search term
            limit: Maximum results

        Returns:
            List of company dicts
        """
        repo = self._factory.get_company_repository()
        companies = await repo.search_companies(query, limit)
        return [company_to_list_dict(c) for c in companies]

    async def get_fundamentals(
        self,
        ticker: str,
        timeframe: str = "quarterly",
        limit: int = 4,
    ) -> dict[str, Any]:
        """Get fundamentals (income, balance, cash flow).

        Args:
            ticker: Stock symbol
            timeframe: "quarterly" or "annual"
            limit: Number of periods

        Returns:
            Dict with balance_sheets, cash_flows, income_statements keys
        """
        repo = self._factory.get_fundamental_repository()

        income = await repo.get_income_statements(ticker, timeframe, limit)
        balance = await repo.get_balance_sheets(ticker, timeframe, limit)
        cash = await repo.get_cash_flows(ticker, timeframe, limit)

        return {
            "income_statements": [income_statement_to_dict(i) for i in income],
            "balance_sheets": [balance_sheet_to_dict(b) for b in balance],
            "cash_flows": [cash_flow_to_dict(c) for c in cash],
        }
