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
    get_indicator_metadata,
    get_latest_price,
    get_latest_price_async,
    get_latest_technical_indicators,
    get_live_price_async,
    get_stock_prices,
    get_stock_prices_async,
    get_technical_indicators,
    list_technical_indicators,
    screen_by_technical_indicators,
)
from .movers import get_market_breadth, get_top_movers, get_volume_leaders
from .scanner import scan_ytd_performance
from .schema import describe_database, describe_table
from .screener import screen_stocks
from .sectors import get_sector_performance, list_sectors

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
    "get_live_price_async",
    # Technical indicators
    "get_technical_indicators",
    "get_latest_technical_indicators",
    "screen_by_technical_indicators",
    "get_indicator_metadata",
    "list_technical_indicators",
    # Scanner
    "scan_ytd_performance",
    # Schema discovery
    "describe_database",
    "describe_table",
    # Sectors
    "list_sectors",
    "get_sector_performance",
    # Market movers
    "get_top_movers",
    "get_volume_leaders",
    "get_market_breadth",
    # Screener
    "screen_stocks",
]
