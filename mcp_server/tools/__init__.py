"""MCP server tools package."""

from .companies import (
    get_company_details,
    get_company_details_async,
    list_companies,
    search_companies,
    search_companies_async,
)
from .economy import get_economy_dashboard, get_economy_data, get_economy_data_async
from .fundamentals import get_fundamentals, get_fundamentals_async
from .market_data import (
    get_financial_ratios,
    get_financial_ratios_async,
    get_latest_price,
    get_latest_price_async,
    get_stock_prices,
    get_stock_prices_async,
)

__all__ = [
    # Companies
    "list_companies",
    "get_company_details",
    "get_company_details_async",
    "search_companies",
    "search_companies_async",
    # Economy
    "get_economy_data",
    "get_economy_data_async",
    "get_economy_dashboard",
    # Fundamentals
    "get_fundamentals",
    "get_fundamentals_async",
    # Market data
    "get_stock_prices",
    "get_stock_prices_async",
    "get_financial_ratios",
    "get_financial_ratios_async",
    "get_latest_price",
    "get_latest_price_async",
]
